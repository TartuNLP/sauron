import logging
import time
import uuid
import socket
import json
import sys

from flask import Flask, request, jsonify
from multiprocessing import Process, Manager, Queue, log_to_stderr, current_process, cpu_count
from threading import Thread, Lock
from configparser import ConfigParser
from itertools import product

from helpers import UUIDConverter, ThreadSafeDict, Error
from helpers import tokenize, is_json

app = Flask(__name__)

@app.errorhandler(Error)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

@app.route("/", methods=['POST', 'GET'])
def hello():
    return "<h1 style='color:blue'>Translation API</h1>"

@app.route('/translateconf', methods=['POST', 'GET'])
def check_options():
    auth = request.args.get('auth')
    if auth in parser.options('token2domain'):
        domain = parser.get('token2domain', auth)
        for name, c in connections.items():
            if name.startswith(domain):
                return jsonify(c.options)

    else:
        raise Error(f'Authentication is missing or failed', status_code=400)

@app.route('/translate', methods=['POST', 'GET'])
def post_job():
    querry = dict(zip(params, map(request.args.get, params)))
    try:
        body = request.data.decode('utf-8')
    except:
        raise Error(f'utf-8 decoding error')

    if is_json(body) == True:
        body = json.loads(body)
        if 'text' not in body.keys():
            raise Error(f'Please fill in text field to translate')
        body = body['text']

    if body:
        pass
    else:
        if 'text' not in querry.keys():
            raise Error('No text provided for translation')
        body = querry['text']

    for k,v in querry.items():
        if k not in ['odomain','text']:
            if v is None:
                raise Error(f'{k} not found in request', status_code=400)

    if querry['auth'] in parser.options('token2domain'):
        domain = parser.get('token2domain', querry['auth'])
    else:
        raise Error(f'Authentication failed', status_code=400)

    available_servers = False
    for name, c in connections.items():
        if name.startswith(domain):
            if querry['odomain'] not in c.options:
                available_styles = list(c.options)
                querry['odomain'] = available_styles[0]
            if querry['olang'] not in c.options[querry['odomain']].values():
                raise Error(f'Language is not supported', status_code=400)
            if c.connected:
                available_servers = True

    if not available_servers:
        raise Error(f'Server for {domain} domain is not connected', status_code=503)

    sentences = tokenize(body)
    n_sentences = len(sentences)
    params_str = '{}_{}'.format(querry['olang'], querry['odomain'])
    job_id = uuid.uuid1()

    with queues_per_domain as q:
        RESULTS[job_id] = manager.dict()
        RESULTS[job_id]['n_sen'] = n_sentences
        RESULTS[job_id]['status'] = 'posted'
        RESULTS[job_id]['text'] = ''
        q[domain].put((job_id, params_str, sentences))
        logging.info(f'Job with id {job_id} POSTED to the {domain} queue')
        logging.info(f'{q[domain].qsize()} jobs in the {domain} queue')

    while True:
        if job_id in RESULTS:
            if RESULTS[job_id]['status'] == 'done':
                return jsonify({'status': 'done', 'input': body['text'], 'result': RESULTS[job_id]['text']})
        else:
            continue

    return str(job_id)

class Worker(Process):

    def __init__(self, name, request_queue, host, port, output_options):
        super(Worker, self).__init__(name=name)
        self.name = name
        self.connected = False
        self.queue = request_queue
        self.host = host
        self.port = port
        self.options = dict()
        self.requests = ThreadSafeDict()
        self.lock = Lock()

        prod = []
        for k,v in output_options.items():
            langs = v.split(',')
            self.options[k] = {i:l for i, l in enumerate(langs)}
            prod.extend(product([k],langs))

        for pair in prod:
            params = f'{pair[0]}_{pair[1]}'
            with self.requests as r:
                r[params] = ThreadSafeDict()
                with r[params] as r_:
                    r_['job_ids'] = Queue()
                    r_['n_sentences'] = 0
                    r_['text'] = ''

    def consume_queue(self, batch_size=32):
        while True:
            if not self.queue.empty():
                job_id, params_str, src = self.queue.get()
                with self.requests as r:
                    for sentence in src:
                        while True:
                            if r[params_str]['n_sentences'] <= batch_size:
                                r[params_str]['text'] += sentence  # Add text by sentences
                                r[params_str]['n_sentences'] += 1
                                r[params_str]['job_ids'].put(job_id)  # Add id by sentence
                                break
                            else:
                                time.sleep(0.05)
                                continue

    def translate(self, params_str, requests, sock):
        while True:
            with requests as r:
                olang, ostyle = params_str.split('_')
                text = r['text']
                msg = {"src": text, "conf": "{},{}".format(olang, ostyle)}
                jmsg = bytes(json.dumps(msg), 'ascii')
                if self.connected:
                    with self.lock:
                        sock.sendall(jmsg)
                        rawresponse = sock.recv(65536)
                        response = json.loads(rawresponse)
                    responses = tokenize(response)

                # Now response contains bunch of translations
                # Need to assign pieces to respective RESULTS[job]

                r['job_ids'].put(None)
                for i, j in enumerate(iter(r['job_ids'].get, None)):
                    # try:
                    RESULTS[j]['text'] += responses[i]

                    # except IndexError:
                    #
                    # except KeyError:
                    #

                    if RESULTS[j]['n_sen'] == len(tokenize(RESULTS[j]['text'])):
                        RESULTS[j]['status'] = 'done'

                r['n_sentences'] = 0
                r['text'] = ''

    def send_messege(self, msg):
            if not self.queue.empty():
                job_id, params_str, src = self.queue.get()
                RESULTS[job_id]['text'] = msg
                RESULTS[job_id]['status'] = 'done'

    def run(self):

        p = current_process()
        logging.debug(f'Name of spawned process: {p.name}')
        logging.debug(f'ID of spawned process: {p.pid}')
        sys.stdout.flush()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sock.settimeout(2)

        while True:
            try:
                sock.connect((self.host, self.port))
                sock.sendall(b'HI')
                preresponse = sock.recv(5)
                if preresponse == b'OK':
                    self.connected = True
                    logging.debug(f'Connection to {self.host}:{self.port} established')

            except ConnectionRefusedError:
                self.connected = False
                logging.debug(f'Connection to {p.name} refused')
                time.sleep(3600)
                continue

            # except socket.timeout:
            #     self.connected = False
            #     logging.debug(f'Connection to {p.name} timeout')
            #     time.sleep(3600)
            #     continue

            break

        if self.connected == True:
            t1 = Thread(name='Consumer', target=self.consume_queue)
            t2 = list()
            for pair in self.prod:
                params = f'{pair[0]}_{pair[1]}'
                t2.append(Thread(name=params, target=self.translate, args=(params, self.requests[params], sock)))
            t1.daemon = True
            t1.start()
            for t in t2:
                t.daemon = True
                t.start()


if __name__ == '__main__':

    app.url_map.converters['uuid'] = UUIDConverter
    logger = log_to_stderr()
    logger.setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(levelname)s](%(threadName)-10s)%(message)s',
    )

    parser = ConfigParser()
    parser.read('dev.ini')
    params = ['text', 'auth','olang','odomain']

    with open('./config.json') as config_file:
        config = json.load(config_file)

    connections = dict()
    queues_per_domain = ThreadSafeDict()
    manager = Manager()
    RESULTS = manager.dict()


    for domain, conf in config.items():
        queues_per_domain[domain] = Queue()
        engines = conf['Workers']
        for worker, settings in engines.items():
            name = f'{domain}_{worker}'
            settings['output_options'] = conf['output_options']
            w = Worker(name, queues_per_domain[domain], **settings)
            connections[name] = w

    for n, c in connections.items():
        c.daemon = True
        c.start()


    app.run()

