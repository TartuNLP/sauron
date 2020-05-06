import logging
import time
import uuid
import socket
import json
import sys
import random
import ctypes
import traceback

from flask import Flask, request, jsonify, redirect, url_for
from nltk import sent_tokenize
from multiprocessing import Process, Manager, Queue, Value, Condition, log_to_stderr, current_process, cpu_count
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


@app.route('/')
def hello_():
    return redirect("/v1.2")


@app.route("/v1.2", methods=['GET'])
def hello():
    return "<h1 style='color:blue'>Translation API</h1>"


@app.route('/v1.2/translate/support', methods=['POST', 'GET'])
def check_options():
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
        raise Error(f'Authentication is missing or failed', status_code=400)


@app.route('/v1.2/translate', methods=['POST', 'GET'])
def post_job():
    # pdb.set_trace()
    querry = dict(zip(params, map(request.args.get, params)))

    if querry['auth'] in parser.options('token2domain'):
        domain = parser.get('token2domain', querry['auth'])
    else:
        raise Error(f'Authentication failed', status_code=400)

    for k, v in querry.items():
        if k not in ['odomain', 'src']:
            if v is None:
                raise Error(f'{k} not found in request', status_code=400)

    try:
        body = request.data.decode('utf-8')
    except:
        raise Error(f'utf-8 decoding error')

    if is_json(body):
        body = json.loads(body)
        if 'text' in body:
            if type(body['text']) == list:
                t = 'sentences'
            elif type(body['text']) == str:
                t = 'text'
            body = body['text']
        else:
            raise Error(f'Fill in text field to translate')

    else:
        if not body:
            if not querry['src']:
                raise Error('No text provided for translation')
            body = querry['src']
            t = 'src'
        else:
            t = 'raw'
    available_servers = False
    for name, c in connections.items():
        if name.startswith(domain):
            if querry['odomain'] not in c.options:
                available_styles = list(c.options)
                querry['odomain'] = available_styles[0]
            options = c.options[querry['odomain']]
            if querry['olang'] not in options:
                reverse_lang_mapping = {v: k for k, v in lang_code_mapping.items()}
                if querry['olang'] not in [lang_code_mapping[lang] for lang in options]:
                    raise Error(f'Language is not supported', status_code=400)
                querry['olang'] = reverse_lang_mapping[querry['olang']]

            if status[name]:
                available_servers = True
    # if not available_servers:
    #     raise Error(f'Server for {domain} domain is not connected', status_code=503,
    #                 payload={'status': 'done', 'input': body})
    delete_symbol = lambda body: ''.join(c for c in body if c not in '|')
    if t in ['src', 'text', 'raw']:
        body = delete_symbol(body)
        sentences = sent_tokenize(body)
    else:
        body = list(map(delete_symbol, body))
        sentences = body
    n_sentences = len(sentences)
    params_str = '{}_{}'.format(querry['olang'], querry['odomain'])
    job_id = uuid.uuid1()

    with queues_per_domain as q:
        RESULTS[job_id] = manager.dict()
        RESULTS[job_id]['n_sen'] = n_sentences
        RESULTS[job_id]['status'] = 'posted'
        RESULTS[job_id]['text'] = ''
        RESULTS[job_id]['type'] = t
        q[domain].put((job_id, params_str, sentences))
        logging.debug(f'Job with id {job_id} POSTED to the {domain} queue')
        logging.debug(f'{q[domain].qsize()} jobs in the {domain} queue')

    while True:
        if job_id in RESULTS:
            if RESULTS[job_id]['status'] == 'done':
                if RESULTS[job_id]['type'] == 'sentences':
                    return jsonify({'status': 'done', 'input': body, 'result': sent_tokenize(RESULTS[job_id]['text'])})
                return jsonify({'status': 'done', 'input': body, 'result': RESULTS[job_id]['text']})
            if RESULTS[job_id]['status'] == 'fail':
                return jsonify({'status': 'fail', 'input': body, 'result': RESULTS[job_id]['text']})


class Worker(Process):

    def __init__(self, name, request_queue, host, port, output_options):
        super(Worker, self).__init__(name=name)
        self.name = name
        self.daemon = True
        self.connected = False
        self.queue = request_queue
        self.host = host
        self.port = port
        self.options = dict()
        self.requests = ThreadSafeDict()
        self.lock = Lock()
        self.c = Condition()
        self.ready = False

        prod = []
        for k, v in output_options.items():
            langs = v.split(',')
            self.options[k] = [l for l in langs]
            prod.extend(product([k], langs))
        self.prod = copy(prod)
        for pair in self.prod:
            params = f'{pair[1]}_{pair[0]}'
            with self.requests as r:
                r[params] = ThreadSafeDict()
                with r[params] as r_:
                    r_['job_ids'] = Queue()
                    r_['n_sentences'] = 0
                    r_['text'] = ''

    def consume_queue(self, batch_size=32):
        while True:
            if not self.queue.empty():
                self.ready = False
                job_id, params_str, src = self.queue.get()
                with self.requests as r:
                    with self.c:
                        for sentence in src:
                            while True:
                                if r[params_str]['n_sentences'] <= batch_size:
                                    r[params_str]['text'] += sentence + ' | '  # Add text by sentences
                                    r[params_str]['n_sentences'] += 1
                                    r[params_str]['job_ids'].put(job_id)  # Add id by sentence
                                    break
                                else:
                                    time.sleep(0.05)
                                    continue
                        self.ready = True
                        self.c.notify_all()

    def send_request(self, text, olang, ostyle):
        msg = {"src": text, "conf": "{},{}".format(olang, ostyle)}
        jmsg = bytes(json.dumps(msg), 'ascii')
        if self.connected:
            with self.lock:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(20)
                try:
                    sock.connect((self.host, self.port))
                    sock.sendall(b'HI')
                    preresponse = sock.recv(5)
                    if preresponse == b'okay':
                        sock.sendall(jmsg)
                        rawresponse = sock.recv(65536)
                        if rawresponse.startswith(b"msize:"):
                            inMsgSize = int(rawresponse.strip().split(b":")[1])
                            sock.send(b'OK')
                            rawresponse = sock.recv(inMsgSize + 13)
                        try:
                            response = json.loads(rawresponse)
                        except json.decoder.JSONDecodeError as e:
                            logging.debug('Received broken json', e)
                            logging.debug(rawresponse)
                            return tokenize(text)
                        try:
                            responses = tokenize(response['final_trans'])
                        except KeyError:
                            logging.debug('Response does not contain translation')
                            logging.debug(response)
                            return tokenize(text)
                        return responses
                    else:
                        return(tokenize(text))
                except (ConnectionRefusedError, TimeoutError):
                    return(tokenize(text))
                finally:
                    sock.close()

    def translate(self, params_str, requests):
        olang, ostyle = params_str.split('_')
        olang = lang_code_mapping[olang]
        while True:
            with requests as r:
                with self.c:
                    while not self.ready:
                        self.c.wait()
                    text = r['text']
                    if text != '':

                        # Translation actually happens here
                        responses = self.send_request(text, olang, ostyle)
                        logging.debug(responses)
                        # responses = sent_tokenize(text)

                        # Now response contains bunch of translations
                        # Need to assign pieces to respective RESULTS[job]

                        r['job_ids'].put(None)
                        for i, j in enumerate(iter(r['job_ids'].get, None)):
                            try:
                                RESULTS[j]['text'] += responses[i] + ' '
                            except IndexError:
                                logging.info('IndexError', i, responses)
                                RESULTS[j]['status'] = 'fail'
                            length = len(sent_tokenize(RESULTS[j]['text']))
                            if RESULTS[j]['n_sen'] <= length:
                                RESULTS[j]['status'] = 'done'
                        r['n_sentences'] = 0
                        r['text'] = ''

    def send_message(self, msg):
        if not self.queue.empty():
            job_id, params_str, src = self.queue.get()
            RESULTS[job_id]['text'] = msg
            RESULTS[job_id]['status'] = 'done'

    def check_connection(self, check_every=None):
        p = current_process()
        while True:
            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.connect((self.host, self.port))
                self.socket.sendall(b'HI')
                preresponse = self.socket.recv(5)
                if preresponse == b'okay':
                    self.connected = True
                    status[self.name] = True
                    logging.info(f'Connection to {self.name}, {self.host}:{self.port} established')
                    ostyle, olang = random.choice(list(self.options.items()))
                    msg = {"src": 'check', "conf": "{},{}".format(olang[0], ostyle)}
                    jmsg = bytes(json.dumps(msg), 'ascii')
                    self.socket.sendall(jmsg)
                    rawresponse = self.socket.recv(65536)
                    response = json.loads(rawresponse)
                    break
                else:
                    logging.debug(preresponse)

            except ConnectionRefusedError:
                self.connected = False
                logging.info(f'Connection to {p.name} refused')
                if not check_every:
                    break
                time.sleep(check_every)

            except TimeoutError:
                self.connected = False
                logging.info(f'Connection to {p.name} timeout')
                if not check_every:
                    break
                time.sleep(check_every)

            except Exception as e:
                self.connected = False
                logging.info(traceback.format_exc())
                if not check_every:
                    break
                time.sleep(check_every)

            finally:
                self.socket.close()

    def run(self):
        p = current_process()
        logging.debug(f'Name of spawned process: {p.name}')
        logging.debug(f'ID of spawned process: {p.pid}')
        sys.stdout.flush()
        self.check_connection()
        if self.connected:
            t1 = Thread(name='Consumer', target=self.consume_queue)
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


if __name__ == '__main__':

    app.url_map.converters['uuid'] = UUIDConverter
    app.config['JSON_SORT_KEYS'] = False
    logger = log_to_stderr()
    logger.setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.INFO,
        format='[%(levelname)s](%(threadName)-10s)%(message)s',
    )

    parser = ConfigParser()
    parser.read('dev.ini')
    params = ['src', 'auth', 'olang', 'odomain']
    odomain_code_mapping = {'fml': 'Formal', 'inf': 'Informal', 'auto': 'Auto', 'tt': 'tt', 'cr': 'cr'}
    lang_code_mapping = {'est': 'et', 'lav': 'lv', 'eng': 'en', 'rus': 'ru', 'fin': 'fi', 'lit': 'lt', 'ger': 'de'}

    with open('./config.json') as config_file:
        config = json.load(config_file)

    queues_per_domain = ThreadSafeDict()
    manager = Manager()
    connections = ThreadSafeDict()
    RESULTS = manager.dict()
    status = manager.dict()

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

    app.run(host = 'translate.cloud.ut.ee', port = 80, use_reloader = False)
