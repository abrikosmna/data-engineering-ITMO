import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from kazoo.client import KazooClient

BASE = Path('/app/hw4')
HOSTS = os.getenv('ZK_HOSTS', 'zk1:2181,zk2:2181,zk3:2181')


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()


def connect():
    zk = KazooClient(hosts=HOSTS, timeout=10.0, read_only=False)
    zk.start(timeout=25)
    return zk


def log(name, event, **fields):
    record = dict(time=datetime.now(timezone.utc).isoformat(), timestamp=time.time(),
                  monotonic=time.monotonic(), event=event, **fields)
    line = json.dumps(record, ensure_ascii=False)
    print(line, flush=True)
    path = BASE / 'logs' / f'{name}.jsonl'
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a') as stream:
        stream.write(line + '\n')
        stream.flush()


def initialize(zk, root):
    zk.ensure_path(root)
    for node, data in [('epoch', b'0'), ('checkpoint', encode({'next_chunk': 0})),
                       ('result', b'null'), ('publications', b'')]:
        from kazoo.exceptions import NodeExistsError
        try:
            zk.create(root + '/' + node, data)
        except NodeExistsError:
            pass


def commit(tx):
    results = tx.commit()
    errors = [r for r in results if isinstance(r, Exception)]
    if errors:
        from kazoo.exceptions import BadVersionError
        raise next((e for e in errors if isinstance(e, BadVersionError)), errors[0])
    return results
