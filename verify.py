"""Linux integration checks: bounded queue, exactly-once execution, drain, CLI."""
import json
import pathlib
import subprocess
import tempfile


def terminal_test(log):
    """Use a real pseudo-terminal so the live refresh path is exercised."""
    import errno
    import os
    import pty
    import re
    import select
    import time

    master, slave = pty.openpty()
    proc = None
    output = bytearray()
    try:
        proc = subprocess.Popen(['./lab', 'pool', '2', '24', '50', str(log)],
                                stdout=slave, stderr=subprocess.PIPE,
                                env={**os.environ, 'TERM': 'xterm'})
        os.close(slave)
        slave = None
        deadline = time.monotonic() + 10
        while True:
            assert time.monotonic() < deadline, 'Terminal demo timed out'
            if not select.select([master], [], [], 0.1)[0]:
                continue
            try:
                chunk = os.read(master, 65536)
            except OSError as error:
                if error.errno != errno.EIO:
                    raise
                break
            if not chunk:
                break
            output.extend(chunk)
        assert proc.wait(timeout=2) == 0, proc.stderr.read().decode()
    finally:
        if proc is not None:
            if proc.poll() is None:
                proc.kill()
            proc.wait()
            proc.stderr.close()
        os.close(master)
        if slave is not None:
            os.close(slave)
    frames = output.decode().replace('\r', '').split('\x1b[H')[1:]
    assert len(frames) >= 3, 'Expected multiple live refreshes'
    for frame in frames:
        # Home alone leaves stale characters; erase must precede new text.
        assert frame.startswith('\x1b[JCONCURRENCY LAB')
        queued, completed = map(int, re.search(r'Queued: (\d+)/16   Completed: (\d+)/24', frame).groups())
        assert 0 <= completed <= 24
        waiting = re.search(r'Waiting jobs:([^\n]*)', frame).group(1).strip()
        ids = [] if waiting == '(empty)' else list(map(int, waiting.split()))
        assert len(ids) == queued and len(ids) == len(set(ids))
        assert all(0 <= job < 24 for job in ids)
    assert 'Completed: 24/24' in frames[-1]
    assert 'Waiting jobs: (empty)' in frames[-1]
    validate([json.loads(line) for line in log.read_text().splitlines()], 24, 2)


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
        terminal_test(log)
    print('PASS: queue accounting, exactly-once jobs, clean shutdown, demos, invalid input and live terminal refresh')
