"""API maintains queries to neural machine translation servers.

https://github.com/TartuNLP/sauron

Examples:
    To run as a standalone script:
        $ python /path_to/sauron.py

    To deploy with Gunicorn refer to WSGI callable from this module:
        $ gunicorn [OPTIONS] sauron:app

Attributes:
    app (flask.app.Flask):
        central Flask object
    req_params (list):
        Specifies expected request parameters
    connections (dict):
        Dictionary of worker instances
    lang_code_mapping (dict):
        3-letter to 2-letter language code mapping
    odomain_code_mapping (dict):
        Output domain code to name mapping
    queues_per_domain (helpers.ThreadSafeDict):
        Dictionary of queues for the main process
    manager (multiprocessing.managers.SyncManager):
        Manager for creating shared memory across sub-processes
    status ('multiprocessing.managers.DictProxy'):
        Stores servers availability flag
    RESULTS ('multiprocessing.managers.DictProxy'):
        Stores the result for every job id
"""

import logging
import time
import uuid
import socket
import json
import sys
import random
import traceback
from flask import Flask, request, jsonify, redirect
from nltk import sent_tokenize
from multiprocessing import Process, Manager, Queue, Condition, current_process
from threading import Thread, Lock
from configparser import ConfigParser
from itertools import product
from copy import copy
from helpers import UUIDConverter, ThreadSafeDict, Error
from helpers import is_json, tokenize

app = Flask(__name__)


@app.errorhandler(Error)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@app.route("/v1.2", methods=['GET'])
def hello():
    return "<h1 style='color:blue'>Translation API</h1>"


@app.route('/')
def hello_():
    return redirect("/v1.2")


@app.route('/v1.2/translate/support', methods=['POST', 'GET'])
def check_options():
    """Informs user with available options for the translation.

    Requires authentication key to provide specific settings
    predefined for the first worker in the attached domain.

    Args:
        Flask request proxy for matched endpoint

    Returns:
        JSON: contains domain name,
        languages and output domain provided for the translation

        Example:
            {
              "domain": "{DOMAIN_NAME}",
              "options": [
                {
                  "odomain": "{OUTPUT_DOMAIN_CODE}",
                  "name": "{OUTPUT_DOMAIN_NAME}",
                  "lang": [
                    "{LANG_1}",
                    "{LANG_2}",
                    "{LANG_3}"
                  ]
                }
              ]
            }

    Raises:
        [400] Authentication error
    """

    auth = request.args.get('auth')
    if auth in parser.options('token2domain'):
        domain = parser.get('token2domain', auth)
        for name, c in connections.items():
            if name.startswith(domain):
                std_fmt = [{"odomain": option,
                            "name": odomain_code_mapping[option],
                            "lang": c.options[option]} for option in c.options]
                return jsonify({"domain": c.name.split('_')[0], "options": std_fmt})
    else:
        raise Error(f'Authentication is missing or failed', payload={'status': 'fail'}, status_code=400)


@app.route('/v1.2/translate', methods=['POST', 'GET'])
def post_job():
    """Post translation request to the queue manager.

    If there are any available workers for the corresponding domain,
    put the job with unique id into the queue and wait for the result.

    Args:
        Flask request proxy for matched endpoint

    Returns:
        JSON: Contains response status, input text,
        result or error message.

        Example:
            {
                "status": "done",
                "input": [
                    "Hello",
                    "World!"
                ],
                "result": [
                    "Tere.",
                    "Maailm!"
                ]
            }

    Raises:
        [400] Authentication error
        [400] Obligatory parameter not in request error
        [400] Text is not included error
        [400] Language is not supported error
        [400] utf-8 decoding error
        [400] 'text' field skipped in input JSON error
        [503] Server in not available error

    """

    # Logging request details
    app.logger.info('Arguments: %s', request.args)
    app.logger.info('Body: %s', request.get_data())

    # Get arguments of interest into a dictionary
    # where req_params is a global requirement
    querry = dict(zip(req_params, map(request.args.get, req_params)))

    # Try get domain corresponding to the auth key
    if querry['auth'] in parser.options('token2domain'):
        domain = parser.get('token2domain', querry['auth'])
    else:
        raise Error(f'Authentication failed', payload={'status': 'fail'}, status_code=400)

    for k, v in querry.items():
        # Optional params can be omitted
        if k not in ['odomain', 'src']:
            # But if any required arguments are missing
            if v is None:
                raise Error(f'{k} not found in request', payload={'status': 'fail'}, status_code=400)

    # Decode HTTP request body data
    try:
        body = request.data.decode('utf-8')
    except Exception:
        raise Error(f'utf-8 decoding error', payload={'status': 'fail'}, status_code=400)

    # If there is a valid JSON in the body data
    if is_json(body):
        body = json.loads(body)
        # then obtain text rather raw or as segments.
        if 'text' in body:
            if type(body['text']) == list:
                t = 'sentences'
            elif type(body['text']) == str:
                t = 'text'
            body = body['text']
        else:
            raise Error(f'Fill in text field to translate', payload={'status': 'fail'}, status_code=400)

    # Otherwise try getting it from src argument
    else:
        if not body:
            if not querry['src']:
                raise Error('No text provided for translation', payload={'status': 'fail'}, status_code=400)
            body = querry['src']
            t = 'src'
        else:
            t = 'raw'

    # Verify validity of arguments and capability of handling request
    available_servers = False
    for name, c in connections.items():
        if name.startswith(domain):
            # If requested output domain is not in the supported list
            # for current worker allocate first as such
            if querry['odomain'] not in c.options:
                available_odomains = list(c.options)
                querry['odomain'] = available_odomains[0]
            options = c.options[querry['odomain']]
            # Look for language 2;3 -letter mappings
            if querry['olang'] not in options:
                reverse_lang_mapping = {v: k for k, v in lang_code_mapping.items()}
                if querry['olang'] not in [lang_code_mapping[lang] for lang in options]:
                    raise Error(f'Language is not supported', payload={'status': 'fail'}, status_code=400)
                # Still translate function waits for 3-letter code
                querry['olang'] = reverse_lang_mapping[querry['olang']]
            # Check whether expected backend reachable
            if status[name]:
                available_servers = True
    if not available_servers:
        raise Error(f'Server for {domain} domain is not connected', status_code=503,
                    payload={'status': 'fail', 'input': body})

    # Get rid of separator from the original text
    # to avoid further conflicts with tokenizer
    delete_symbol = lambda body: ''.join(c for c in body if c not in '|')

    # Delete depends on input type
    if t in ['src', 'text', 'raw']:
        body = delete_symbol(body)
        sentences = sent_tokenize(body)
    else:
        body = list(map(delete_symbol, body))
        sentences = body

    n_sentences = len(sentences)
    params_str = '{}_{}'.format(querry['olang'], querry['odomain'])
    job_id = uuid.uuid1()

    # Pass parameters further to the queue manager
    with queues_per_domain as q:
        RESULTS[job_id] = manager.dict()
        RESULTS[job_id]['n_sen'] = n_sentences
        RESULTS[job_id]['status'] = 'posted'
        RESULTS[job_id]['text'] = ''
        RESULTS[job_id]['type'] = t
        q[domain].put((job_id, params_str, sentences))
        app.logger.debug(f'Job with id {job_id} POSTED to the {domain} queue')
        app.logger.debug(f'{q[domain].qsize()} jobs in the {domain} queue')

    # Wait until results are updated
    while True:
        if job_id in RESULTS:
            if RESULTS[job_id]['status'] == 'done':
                if RESULTS[job_id]['type'] == 'sentences':
                    response = jsonify({'status': 'done', 'input': body,
                                        'result': sent_tokenize(RESULTS[job_id]['text'])})
                    return response
                response = jsonify({'status': 'done', 'input': body, 'result': RESULTS[job_id]['text']})
                return response
            if RESULTS[job_id]['status'].startswith('fail'):
                response = jsonify({'status': RESULTS[job_id]['status'],
                                    'input': body,
                                    'message': RESULTS[job_id]['message']})
                return response


class Worker(Process):
    """Performs unique connection to the Nazgul instance.

    Attributes:
        name: String contains {domain_name}_{worker_idx}
        connected: Boolean indicator of worker status
        queue: Separate queue of requests
        host: Nazgul IP address
        port: Nazgul Port
        options: Worker configuration
        requests: Buffer for accumulating similar requests
        lock: Threading lock
        c: Threading condition
        ready: Boolean status of producing thread
        prod: List of all parameters combinations
    """

    def __init__(self,
                 name: str,
                 request_queue: Queue,
                 host: str,
                 port: int,
                 output_options: dict) -> None:

        super(Worker, self).__init__(name=name)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.name = name
        self.connected = False
        self.queue = request_queue
        self.host = host
        self.port = port
        self.options = dict()
        self.requests = ThreadSafeDict()
        self.lock = Lock()
        self.c = Condition()
        self.ready = False

        # Create buffer for processing requests
        prod = []
        for k, v in output_options.items():
            langs = v.split(',')
            # Fill in options from config file
            self.options[k] = [l for l in langs]
            # Calculate all lang/odomain pairs
            prod.extend(product([k], langs))
        self.prod = copy(prod)
        for pair in self.prod:
            params = f'{pair[1]}_{pair[0]}'
            # Extend and init buffer to include all constructed combinations
            with self.requests as r:
                r[params] = ThreadSafeDict()
                with r[params] as r_:
                    r_['job_ids'] = Queue()
                    r_['n_sentences'] = 0
                    r_['text'] = ''

    def produce_queue(self, batch_size: int = 32) -> None:
        """Processes requests from the general queue

        Supervise general queue for the worker.
        After element is retrieved it is directed
        to fill corresponding buffer with sentences one by one.
        When done, release the lock for translation.

        Args:
            batch_size: maximum sentences allowed in buffer

        Returns:
            None
        """

        while True:
            if not self.queue.empty():
                job_id, params_str, src = self.queue.get()
                l = len(src)
                with self.requests as r:
                    for s, sentence in enumerate(src):
                        with self.c:
                            # Wait until buffer is not overloaded
                            while not r[params_str]['n_sentences'] < batch_size:
                                self.c.wait()
                            self.ready = False
                            # Extend given text with a new sentence and sep token
                            r[params_str]['text'] += sentence + '|'
                            # Keep number of sentences in the buffer
                            r[params_str]['n_sentences'] += 1
                            # And their job ids
                            # to combine chunks for every request afterwards
                            r[params_str]['job_ids'].put(job_id)
                            # Continue if buffer has some capacity left
                            # and last sentence in request is not reached yet
                            if (r[params_str]['n_sentences'] < batch_size) and (s < l-1):
                                continue
                            # Otherwise notify translation threads
                            else:
                                self.ready = True
                                self.c.notify_all()

    def send_request(self, text: str, olang: str, odomain: str) -> tuple:
        """Send prepared batch for translation.

        Endpoint receives
        msg = { "src": "hello", "conf": "ger,fml" }
        transferred in bytes via socket communication

        Args:
            text: text to translate
            olang: output language
            odomain: output domain

        Returns:
            Tuple containing response with the translation or an error.
            Type of first element is rather str or bool respectively.
        """

        msg = {"src": text.strip('|'), "conf": "{},{}".format(olang, odomain)}
        jmsg = bytes(json.dumps(msg), 'ascii')
        if self.connected:
            with self.lock:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    sock.connect((self.host, self.port))
                    sock.sendall(b'HI')
                    preresponse = sock.recv(5)
                    if preresponse == b'okay':
                        sock.sendall(jmsg)
                        rawresponse = sock.recv(65536)
                        if rawresponse.startswith(b"msize:"):
                            in_msg_size = int(rawresponse.strip().split(b":")[1])
                            sock.sendall(b'OK')
                            rawresponse = sock.recv(in_msg_size + 13)
                        try:
                            response = json.loads(rawresponse)
                        except json.decoder.JSONDecodeError as e:
                            app.logger.debug('Received broken json', e)
                            app.logger.debug(rawresponse)
                            return False, f'Can not decode server raw response : {response}'
                        try:
                            translation = response['final_trans']
                        except KeyError:
                            app.logger.debug('Response does not contain translation')
                            app.logger.debug(response)
                            return False, f'Server response: {response}'
                        responses = tokenize(translation)
                        return responses
                    else:
                        return False, 'Server is not polite'
                except Exception:
                    return False, traceback.format_exc()
                finally:
                    sock.close()

    def translate(self, params_str: str, requests: ThreadSafeDict) -> None:
        """Continuously get translation for the text from the allocated buffer.

        Args:
            params_str: defines thread purpose "{olang}_{odomain}"
            requests: separate buffer created in class init

        Returns:
            None
        """

        olang, odomain = params_str.split('_')
        olang = lang_code_mapping[olang]

        while True:
            with self.c:
                # Wait until producing thread finishes filling batch
                while not self.ready:
                    self.c.wait()
                # Check whether any text arrived
                # if so get the text and refresh the buffer
                with requests as r:
                    text = r['text']
                    if text:
                        r['job_ids'].put(None)
                        itr = iter(r['job_ids'].get, None)
                        r['n_sentences'] = 0
                        r['text'] = ''
                        self.c.notify_all()
            if text:
                # Translation actually happens here
                responses = self.send_request(text, olang, odomain)
                app.logger.info(responses)
                # Now response contains bunch of translations
                # Need to assign chunks to respective jobs
                self.save_result(responses, itr)
                del text

    @staticmethod
    def save_result(responses: tuple, itr: iter) -> None:
        """Put translated text into result's global object.

        Args:
            responses: Tuple contains server response
            itr: Iterable with a sequence of job id's for every sent sentence

        Returns:
            None
        """

        for i, j in enumerate(itr):
            if type(responses[0]) is bool:
                RESULTS[j]['message'] = responses[1:]
                RESULTS[j]['status'] = 'fail'
            else:
                try:
                    RESULTS[j]['text'] += responses[i] + ' '
                except IndexError:
                    trace = traceback.format_exc()
                    app.logger.error(trace)
                    RESULTS[j]['status'] = 'fail'
                length = len(sent_tokenize(RESULTS[j]['text']))
                if RESULTS[j]['n_sen'] <= length:
                    RESULTS[j]['status'] = 'done'

    def check_connection(self, check_every: int = None) -> None:
        """Update server availability status.

        Ping translation server. By default it is done only when worker is started.
        To regularly send status request set check_every parameter.

        Args:
            check_every: ping how often (s)

        Returns:
            None
        """

        p = current_process()
        while True:
            with self.lock:
                try:
                    self.socket.connect((self.host, self.port))
                    self.socket.sendall(b'HI')
                    preresponse = self.socket.recv(5)
                    if preresponse == b'okay':
                        self.connected = True
                        status[self.name] = True
                        app.logger.info(f'Connection to {self.name}, {self.host}:{self.port} established')
                        odomain, olang = random.choice(list(self.options.items()))
                        msg = {"src": 'check', "conf": "{},{}".format(olang[0], odomain)}
                        jmsg = bytes(json.dumps(msg), 'ascii')
                        self.socket.sendall(jmsg)
                        self.socket.recv(65536)
                        break
                    else:
                        app.logger.debug(preresponse)

                except ConnectionRefusedError:
                    self.connected = False
                    app.logger.info(f'Connection to {p.name} refused')
                    if not check_every:
                        break
                    time.sleep(check_every)

                except TimeoutError:
                    self.connected = False
                    app.logger.info(f'Connection to {p.name} timeout')
                    if not check_every:
                        break
                    time.sleep(check_every)

                except Exception:
                    self.connected = False
                    app.logger.info(traceback.format_exc())
                    if not check_every:
                        break
                    time.sleep(check_every)

                finally:
                    self.socket.close()

    def run(self) -> None:
        """Start worker sub-threads with distributed shared resource.

        Overrides multiprocessing run() method

        Returns:
            None
        """

        p = current_process()
        app.logger.debug(f'Name of spawned process: {p.name}')
        app.logger.debug(f'ID of spawned process: {p.pid}')
        sys.stdout.flush()
        self.check_connection(3600)
        if self.connected:
            t1 = Thread(name='Consumer', target=self.produce_queue)
            t2 = list()
            for pair in self.prod:
                params = f'{pair[1]}_{pair[0]}'
                t2.append(Thread(name=params, target=self.translate, args=(params, self.requests[params])))
            t1.start()
            for t in t2:
                t.start()
            t1.join()
            for t in t2:
                t.join()


# Add hoc settings
app.url_map.converters['uuid'] = UUIDConverter
app.config['JSON_SORT_KEYS'] = False

# Set up logging
gunicorn_logger = logging.getLogger('gunicorn.error')
app.logger.handlers.extend(gunicorn_logger.handlers)
app.logger.setLevel(gunicorn_logger.level)

# Set global variables
queues_per_domain = ThreadSafeDict()
connections = dict()
manager = Manager()
RESULTS = manager.dict()
status = manager.dict()
req_params = ('src', 'auth', 'olang', 'odomain')
lang_code_mapping = {'est': 'et', 'lav': 'lv', 'eng': 'en', 'rus': 'ru', 'fin': 'fi', 'lit': 'lt', 'ger': 'de'}
odomain_code_mapping = {'fml': 'Formal',
                        'inf': 'Informal',
                        'auto': 'Auto',
                        'tt': 'tt',
                        'cr': 'cr',
                        'ep': 'Formal',
                        'os': 'Informal',
                        'pc': 'Auto',
                        'dg': 'dg',
                        'jr': 'jr',
                        'em': 'em',
                        'nc': 'nc',
                        'd1': 'd1'}

# Read config
parser = ConfigParser()
parser.read('./dev.ini')
with open('./config.json') as config_file:
    config = json.load(config_file)

# Start workers
for _, domains in config.items():
    for domain in domains:
        queues_per_domain[domain['name']] = Queue()
        engines = domain['Workers']
        for worker in engines:
            domain_name = domain['name']
            worker_name = worker['name']
            name = f'{domain_name}_{worker_name}'
            worker['settings']['output_options'] = domain['output_options']
            w = Worker(name, queues_per_domain[domain_name], **worker['settings'])
            connections[name] = w
            status[name] = False
            w.start()


if __name__ == '__main__':
    app.run()
