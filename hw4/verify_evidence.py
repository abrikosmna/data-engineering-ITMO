import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent


def load(path):
    return json.loads(path.read_text())


def canonical(counts):
    return sorted((tuple(row['skills']), row['k'], row['count']) for row in counts)


def main():
    ensemble = load(HERE / 'evidence/ensemble.json')
    for stage in ('three','two','restored'):
        assert 'WRITE_COMMITTED' in ensemble[stage]['probe']
    assert 'NO_QUORUM' in ensemble['one']['probe']
    assert sum('Mode: leader' in s for s in ensemble['three']['status'].values()) == 1
    assert sum('Mode: follower' in s for s in ensemble['three']['status'].values()) == 2
    assert not any('Mode: ' in s for s in ensemble['one']['status'].values())
    ephemeral = (HERE / 'evidence/ephemeral.txt').read_text()
    assert 'exists= True' in ephemeral and 'after_session_close_exists= False' in ephemeral
    baselines = [load(ROOT / 'results' / name) for name in ('mr_optimized_20_1.json','duckdb_20_1.json')]
    digests = []
    for run, expected in [('unfenced',2),('fenced',1)]:
        evidence = load(HERE / f'evidence/{run}.json')
        snapshot = evidence['snapshot']
        assert snapshot['pointer_version'] == expected
        assert len(snapshot['publications']) == expected
        assert evidence['saved_checkpoint'] == evidence['resumed_checkpoint'] > 0
        assert 39 <= evidence['paused_seconds'] <= 45
        assert 0 < evidence['failover_seconds_lower'] <= evidence['failover_seconds_upper'] < 25
        digest = snapshot['pointer']['sha256']
        artifact = HERE / 'artifacts' / run / (digest + '.json')
        assert hashlib.sha256(artifact.read_bytes()).hexdigest() == digest
        result = load(artifact)
        for baseline in baselines:
            assert canonical(result['counts']) == canonical(baseline['counts'])
            assert result['vacancies'] == baseline['vacancies']
            assert result['vocabulary'] == baseline['vocabulary']
        assert result['vacancies']==6075 and len(result['counts'])==1330
        assert result['events']=={'1':19307,'2':41058,'3':58679}
        assert result['unique']=={'1':20,'2':190,'3':1120}
        publications, rejections = [], []
        for name in ('coord-a','coord-b'):
            logs = [json.loads(line) for line in (HERE / f'logs/{run}-{name}.jsonl').read_text().splitlines()]
            assert next(r for r in logs if r['event']=='ready')['session_timeout_ms']==10000
            assert any(r['event']=='verified_hw3' and r['all_counts_equal'] for r in logs)
            publications += [r for r in logs if r['event']=='published']
            rejections += [r for r in logs if r['event']=='publication_rejected']
        assert len(publications)==expected
        assert len(rejections)==(1 if run=='fenced' else 0)
        if rejections:
            assert rejections[0]['error']=='BadVersionError'
            assert snapshot['pointer']['coordinator']==evidence['backup']
        digests.append(digest)
        print(f'{run}: publications={expected}, pointer_version={expected}, all 1330 frequencies equal HW3 MapReduce and DuckDB')
    assert len(set(digests))==1
    print('PASS: quorum, ephemeral session, failover, checkpoint, fencing, full HW3 equivalence, artifact SHA256')


if __name__=='__main__':
    main()
