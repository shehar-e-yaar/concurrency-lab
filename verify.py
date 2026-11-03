"""Linux integration checks: bounded queue, exactly-once execution, drain, CLI."""
import json
import pathlib
import subprocess
import tempfile


def validate(events, jobs, workers):
    queued, active, done, stopped = set(), {}, set(), set()
    previous = -1
    for e in events:
        assert e['ms'] >= previous
        previous = e['ms']
        kind, worker, job = e['type'], e['worker'], e['job']
        if kind == 'submit':
            assert 0 <= job < jobs and job not in queued | done
            assert job not in active.values()
            queued.add(job)
        elif kind == 'start':
            assert job in queued and worker not in active
            queued.remove(job)
            active[worker] = job
        elif kind == 'done':
            assert active.pop(worker) == job and job not in done
            done.add(job)
        elif kind == 'stopped':
            assert worker not in active and worker not in stopped
            stopped.add(worker)
        else:
            assert kind == 'idle' and worker not in active
        if worker != -1:
            assert 0 <= worker < workers
        assert e['queue'] == len(queued) <= 16
        assert e['completed'] == len(done)
    assert len(done) == jobs and not queued and not active
    assert stopped == set(range(workers))


if __name__ == '__main__':
    with tempfile.TemporaryDirectory() as folder:
        log = pathlib.Path(folder) / 'events.jsonl'
        for workers in (1, 2, 4, 8):
            for jobs in (0, 1, 17, 500):
                result = subprocess.run(['./lab', 'pool', str(workers), str(jobs), '0', str(log)], check=True, timeout=20, capture_output=True, text=True)
                assert 'CONCURRENCY LAB | PID' in result.stdout
                assert f'Completed: {jobs}/{jobs}' in result.stdout
                assert result.stdout.count('STOPPED') == workers
                assert '\x1b' not in result.stdout
                validate([json.loads(line) for line in log.read_text().splitlines()], jobs, workers)
        for demo in ('race', 'deadlock', 'wait'):
            result = subprocess.run(['./lab', demo], check=True, timeout=20, capture_output=True, text=True)
            if demo == 'race':
                assert 'Atomic increment: 400000 / 400000' in result.stdout
        for args in (['pool', '0'], ['pool', '9'], ['pool', '2', '-1'], ['pool', 'x'], ['unknown']):
            assert subprocess.run(['./lab', *args], capture_output=True, timeout=5).returncode != 0
    print('PASS: queue accounting, exactly-once jobs, clean shutdown, demos and invalid input')
