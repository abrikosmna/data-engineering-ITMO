import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
EVIDENCE = HERE / 'evidence'
EVIDENCE.mkdir(exist_ok=True)
COMPOSE = ['docker', 'compose', '-f', str(HERE / 'compose.yaml')]
TRANSCRIPT = EVIDENCE / 'commands.txt'


def command(args, *, env=None, check=True, input=None):
    started = datetime.now(timezone.utc).isoformat()
    process = subprocess.run(args, text=True, input=input, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, env=env)
    with TRANSCRIPT.open('a') as stream:
        stream.write(f'\n[{started}] $ ' + ' '.join(args) + '\n' + process.stdout +
                     f'\nexit_code={process.returncode}\n')
    if check and process.returncode:
        raise RuntimeError(process.stdout)
    return process


def dc(*args, **kwargs):
    return command(COMPOSE + list(args), **kwargs)


def py(code, **kwargs):
    return dc('exec', '-T', '-w', '/app/hw4', 'receiver', 'python', '-c', code, **kwargs)


def records(run, name):
    path = HERE / 'logs' / f'{run}-{name}.jsonl'
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def wait_for(fn, timeout=60):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = fn()
        if result:
            return result
        time.sleep(.1)
    raise TimeoutError('condition not met')


def probe(hosts, success):
    code = f'''from kazoo.client import KazooClient
z=KazooClient(hosts={hosts!r},timeout=4,read_only=False)
try:
 z.start(timeout=6)
 z.ensure_path('/hw4/quorum')
 p=z.create('/hw4/quorum/probe-',b'committed',sequence=True)
 print('WRITE_COMMITTED',p,z.get(p)[0].decode())
 z.delete(p)
 assert {success!r}, 'unexpected quorum'
except z.handler.timeout_exception:
 print('NO_QUORUM: connection/write unavailable')
 assert not {success!r}, 'expected quorum'
finally:
 z.stop();z.close()
'''
    return py(code).stdout


def ensemble():
    snapshots = {}
    for stage in ('three', 'two', 'one', 'restored'):
        if stage == 'two':
            dc('stop', 'zk3')
        elif stage == 'one':
            dc('stop', 'zk2')
        elif stage == 'restored':
            dc('start', 'zk2', 'zk3')
        time.sleep(4)
        if stage == 'restored':
            def restored_roles():
                states = [dc('exec', '-T', node, 'zkServer.sh', 'status', check=False).stdout for node in ('zk1','zk2','zk3')]
                return all('Mode: ' in state for state in states)
            wait_for(restored_roles, timeout=70)
        statuses = {}
        for server in ('zk1', 'zk2', 'zk3'):
            statuses[server] = dc('exec', '-T', server, 'zkServer.sh', 'status', check=False).stdout
        snapshots[stage] = dict(status=statuses, probe=probe('zk1:2181', stage != 'one'))
        print(stage, snapshots[stage]['probe'], flush=True)
    (EVIDENCE / 'ensemble.json').write_text(json.dumps(snapshots, ensure_ascii=False, indent=2))
    dc('ps', '-a')
    for server in ('zk1', 'zk2', 'zk3'):
        result = dc('exec', '-T', server, 'sh', '-c', 'cat /conf/zoo.cfg; echo myid; cat /data/myid; java -version')
        (EVIDENCE / f'{server}-configuration.txt').write_text(result.stdout)
    # A real zkCli session creates the ephemeral node. Another client observes it.
    with (EVIDENCE / 'zkcli-ephemeral.txt').open('w') as output:
        cli = subprocess.Popen(COMPOSE + ['exec', '-T', 'zk2', 'zkCli.sh', '-server', 'zk2:2181'],
                               stdin=subprocess.PIPE, stdout=output, stderr=subprocess.STDOUT, text=True)
        try:
            cli.stdin.write('create -e /hw4_ephemeral session-owned\n')
            cli.stdin.flush()
            time.sleep(4)
            before = py("from shared import connect; z=connect(); s=z.exists('/hw4_ephemeral'); print('exists=',bool(s),'ephemeralOwner=',s.ephemeralOwner if s else None); assert s and s.ephemeralOwner; z.stop(); z.close()")
            cli.stdin.write('quit\n')
            cli.stdin.flush()
            cli.wait(timeout=15)
            after = py("from shared import connect; z=connect(); z.sync('/'); s=z.exists('/hw4_ephemeral'); print('after_session_close_exists=',bool(s)); assert s is None; z.stop(); z.close()")
        finally:
            if cli.poll() is None:
                cli.kill()
                cli.wait()
    (EVIDENCE / 'ephemeral.txt').write_text(before.stdout + after.stdout)


def experiment(run, fencing):
    if any((HERE / 'logs').glob(run + '-*.jsonl')):
        raise RuntimeError('Existing run logs: preserve them, use a fresh evidence directory for reruns')
    env = dict(os.environ, RUN_ID=run, FENCING=str(fencing))
    py(f"from shared import connect,initialize; z=connect(); initialize(z,'/hw4/{run}'); assert z.get('/hw4/{run}/epoch')[1].version==0; z.stop(); z.close()")
    dc('up', '-d', '--force-recreate', 'coord-a', 'coord-b', env=env)
    leader = wait_for(lambda: next((name for name in ('coord-a','coord-b')
                     if any(r['event']=='became_leader' for r in records(run,name))),None))
    backup = 'coord-b' if leader == 'coord-a' else 'coord-a'
    wait_for(lambda: any(r['event']=='ready' for r in records(run,backup)))
    wait_for(lambda: any(r['event']=='checkpoint_saved' and r['next_chunk']>=3 for r in records(run,leader)))
    before = float(py('import time; print(time.monotonic())').stdout.strip())
    wall_before = time.time()
    dc('pause', leader)
    wall_paused = time.time()
    after = float(py('import time; print(time.monotonic())').stdout.strip())
    checkpoint_at_pause = json.loads(py(f"from shared import connect; import json; z=connect(); z.sync('/hw4/{run}/checkpoint'); print(z.get('/hw4/{run}/checkpoint')[0].decode()); z.stop(); z.close()").stdout)
    try:
        became = wait_for(lambda: next((r for r in records(run,backup) if r['event']=='became_leader'),None),timeout=25)
        published = wait_for(lambda: next((r for r in records(run,backup) if r['event']=='published'),None),timeout=30)
        remaining = 40 - (time.time() - wall_paused)
        if remaining > 0:
            time.sleep(remaining)
    finally:
        dc('unpause', leader)
    wall_resumed = time.time()
    wait_for(lambda: any(r['event']=='finished' for r in records(run,leader)),timeout=60)
    wait_for(lambda: any(r['event']=='finished' for r in records(run,backup)),timeout=15)
    snapshot = json.loads(py(f"""import json
from shared import connect
z=connect(); root='/hw4/{run}'
p,s=z.get(root+'/result')
receipts=[json.loads(z.get(root+'/publications/'+c)[0]) for c in sorted(z.get_children(root+'/publications'))]
c=json.loads(z.get(root+'/checkpoint')[0])
print(json.dumps(dict(pointer=json.loads(p),pointer_version=s.version,publications=receipts,checkpoint_next_chunk=c['next_chunk'],epoch_version=z.get(root+'/epoch')[1].version)))
z.stop();z.close()
""").stdout)
    resumed = next(r for r in records(run,backup) if r['event']=='resumed_checkpoint')
    assert resumed['next_chunk'] == checkpoint_at_pause['next_chunk'] > 0
    assert all(any(r['event']=='verified_hw3' and r['all_counts_equal'] for r in records(run,n)) for n in (leader,backup))
    assert snapshot['pointer_version'] == (1 if fencing else 2)
    assert len(snapshot['publications']) == (1 if fencing else 2)
    assert snapshot['pointer']['coordinator'] == (backup if fencing else leader)
    if fencing:
        assert any(r['event']=='publication_rejected' and r['error']=='BadVersionError' for r in records(run,leader))
    outcome = dict(run=run, fencing=fencing, leader=leader, backup=backup,
                   pause_requested_at=wall_before, pause_completed_at=wall_paused,
                   resumed_at=wall_resumed, paused_seconds=wall_resumed-wall_paused,
                   failover_seconds_lower=became['monotonic']-after,
                   failover_seconds_upper=became['monotonic']-before,
                   backup_became_leader=became, backup_published=published,
                   saved_checkpoint=checkpoint_at_pause['next_chunk'], resumed_checkpoint=resumed['next_chunk'],
                   snapshot=snapshot)
    (EVIDENCE / f'{run}.json').write_text(json.dumps(outcome,ensure_ascii=False,indent=2))
    print(json.dumps(outcome,ensure_ascii=False,indent=2),flush=True)
    dc('logs', '--no-color', 'coord-a', 'coord-b')


def main():
    if '--coordinators-only' not in sys.argv:
        ensemble()
    experiment('unfenced',0)
    experiment('fenced',1)
    for server in ('zk1','zk2','zk3','receiver'):
        (EVIDENCE / f'{server}-docker.log').write_text(dc('logs','--no-color',server).stdout)
    (EVIDENCE / 'docker-images.txt').write_text(command(['docker','image','inspect','zookeeper:3.9.6-jre-17','itmo-zk-coordinator:hw4']).stdout)
    print('ALL ACCEPTANCE CHECKS PASSED',flush=True)


if __name__ == '__main__':
    main()
