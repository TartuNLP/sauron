import threading
from werkzeug import routing
import uuid
import json

class ThreadSafeDict(dict) :
    def __init__(self, * p_arg, ** n_arg) :
        dict.__init__(self, * p_arg, ** n_arg)
        self._lock = threading.Lock()

    def __enter__(self) :
        self._lock.acquire()
        return self

    def __exit__(self, type, value, traceback) :
        self._lock.release()

class UUIDConverter(routing.BaseConverter):
    @staticmethod
    def to_python(value):
        try:
            return uuid.UUID(value)
        except ValueError:
            raise routing.ValidationError
    @staticmethod
    def to_url(value):
        return str(value)

class Error(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv


def tokenize(text):
    sentences = tuple(filter(str.strip, text.split('|')))
    return sentences

def is_json(myjson):
    try:
        json_object = json.loads(myjson)
    except ValueError as e:
        return False
    return True