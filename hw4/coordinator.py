import argparse
import csv
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ProcessPoolExecutor

from common import selected, skills
from mapreduce import mapper
from kazoo.exceptions import BadVersionError, ConnectionLoss, NodeExistsError, SessionExpiredError
from shared import BASE, commit, connect, encode, initialize, log


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', required=True)
    args = parser.parse_args()
    name, run = args.name, os.environ['RUN_ID']
    fencing = int(os.environ.get('FENCING', '0'))
    if (run == 'fenced') != bool(fencing):
        raise ValueError('FENCING must match receiver policy for this run')
    tag, root = f'{run}-{name}', '/hw4/' + run
    zk = connect()
    zk.add_listener(lambda state: log(tag, 'session', state=state))
    initialize(zk, root)
    log(tag, 'ready', session_timeout_ms=zk._session_timeout, fencing=fencing,
        session_id=zk.client_id[0])
    while True:
        if json.loads(zk.get(root + '/result')[0]) is not None:
            log(tag, 'already_complete')
            zk.stop()
            zk.close()
            return
        try:
            zk.create(root + '/leader', encode({'name': name}), ephemeral=True)
            break
        except NodeExistsError:
            time.sleep(0.1)
    _, stat = zk.get(root + '/epoch')
    epoch = zk.set(root + '/epoch', encode({'name': name}), version=stat.version).version
    log(tag, 'became_leader', epoch=epoch)
    with open('/app/data/vacancies.csv', newline='', encoding='utf-8') as stream:
        rows = [row for row in csv.DictReader(stream) if selected(row)]
    frequencies = Counter(skill for row in rows for skill in skills(row['key_skills']))
    vocabulary = sorted(frequencies, key=lambda skill: (-frequencies[skill], skill))[:20]
    source_sha = hashlib.sha256(open('/app/data/vacancies.csv', 'rb').read()).hexdigest()
    contract = hashlib.sha256(encode(dict(source=source_sha, vocabulary=vocabulary,
                                         chunk_rows=100, algorithm='hw3-mapper-v1'))).hexdigest()
    checkpoint = json.loads(zk.get(root + '/checkpoint')[0])
    start = checkpoint['next_chunk']
    if start and checkpoint['contract'] != contract:
        raise ValueError('checkpoint belongs to different input or algorithm')
    total = Counter({tuple(row['skills']): row['count'] for row in checkpoint.get('counts', [])})
    events = Counter({int(k): v for k, v in checkpoint.get('events', {}).items()})
    vacancies = checkpoint.get('vacancies', 0)
    log(tag, 'resumed_checkpoint', next_chunk=start, vacancies=vacancies, contract=contract)
    chunks = [rows[i:i + 100] for i in range(0, len(rows), 100)]
    with ProcessPoolExecutor(max_workers=2) as pool:
        for index in range(start, len(chunks)):
            counts, batch_vacancies, batch_events = pool.submit(mapper, chunks[index], set(vocabulary), True).result()
            total.update(counts)
            events.update(batch_events)
            vacancies += batch_vacancies
            current = dict(next_chunk=index + 1, contract=contract, vacancies=vacancies,
                           events=dict(events), counts=[dict(skills=list(k), count=v) for k, v in sorted(total.items())])
            try:
                tx = zk.transaction()
                tx.check(root + '/epoch', version=epoch)
                tx.set_data(root + '/checkpoint', encode(current))
                commit(tx)
                log(tag, 'checkpoint_saved', next_chunk=index + 1, vacancies=vacancies, epoch=epoch)
            except (BadVersionError, ConnectionLoss, SessionExpiredError) as error:
                # Keep stale local work alive to demonstrate publication fencing.
                log(tag, 'stale_checkpoint_skipped', next_chunk=index + 1, epoch=epoch, reason=type(error).__name__)
            time.sleep(0.35)
    result = dict(vacancies=vacancies, vocabulary=vocabulary,
                  counts=[dict(skills=list(k), k=len(k), count=v)
                          for k, v in sorted(total.items(), key=lambda item: (len(item[0]), item[0]))],
                  events=dict(events), unique=dict(Counter(map(len, total))))
    for baseline_name in ('mr_optimized_20_1.json', 'duckdb_20_1.json'):
        baseline = json.load(open('/app/baseline/' + baseline_name))
        assert result['counts'] == sorted(baseline['counts'], key=lambda r: (r['k'], r['skills']))
        assert vacancies == baseline['vacancies'] and vocabulary == baseline['vocabulary']
    log(tag, 'verified_hw3', vacancies=vacancies, events=dict(events), unique=result['unique'],
        all_counts_equal=True, baselines=['MapReduce', 'DuckDB'], source_sha256=source_sha)
    request = urllib.request.Request('http://receiver:8000',
        data=encode(dict(run=run, name=name, epoch=epoch, result=result)),
        headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            published = json.load(response)
        log(tag, 'published', message='записал результат', **published)
    except urllib.error.HTTPError as error:
        rejected = json.load(error)
        log(tag, 'publication_rejected', **rejected)
        if not fencing or rejected['error'] != 'BadVersionError':
            raise
    zk.stop()
    zk.close()
    log(tag, 'finished')


if __name__ == '__main__':
    main()
