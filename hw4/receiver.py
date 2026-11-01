import hashlib
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from shared import BASE, commit, connect, encode, log

zk = connect()


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            run = request['run']
            if run not in ('unfenced', 'fenced'):
                raise ValueError('unknown run')
            root = '/hw4/' + run
            payload = request['result']
            body = encode(payload)
            digest = hashlib.sha256(body).hexdigest()
            folder = BASE / 'artifacts' / run
            folder.mkdir(parents=True, exist_ok=True)
            target = folder / (digest + '.json')
            try:
                with target.open('xb') as stream:
                    stream.write(body)
            except FileExistsError:
                if target.read_bytes() != body:
                    raise ValueError('artifact hash mismatch')
            pointer = dict(coordinator=request['name'], epoch=request['epoch'],
                           sha256=digest, artifact=str(target), run=run)
            tx = zk.transaction()
            if run == 'fenced':
                tx.check(root + '/epoch', version=request['epoch'])
            tx.set_data(root + '/result', encode(pointer))
            tx.create(root + '/publications/item-', encode(pointer), sequence=True)
            commit(tx)
            _, stat = zk.get(root + '/result')
            log('receiver', 'accepted', run=run, **{k: v for k, v in pointer.items() if k != 'run'},
                pointer_version=stat.version)
            response, status = dict(ok=True, pointer=pointer, pointer_version=stat.version), 200
        except Exception as error:
            log('receiver', 'rejected', error=type(error).__name__, detail=str(error))
            response, status = dict(ok=False, error=type(error).__name__), 409
        body = encode(response)
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


log('receiver', 'ready')
ThreadingHTTPServer(('0.0.0.0', 8000), Receiver).serve_forever()
