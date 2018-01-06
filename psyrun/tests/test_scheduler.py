import subprocess
import sys

from psyrun.scheduler import ImmediateRun, JobStatus, Slurm


def test_immediate_run_submit(tmpdir):
    outfile = str(tmpdir) + '/out'
    sched = ImmediateRun()
    sched.submit(['echo', 'success'], outfile)
    with open(outfile, 'r') as f:
        assert f.read().strip() == 'success'


def test_immediate_run_failing_job(tmpdir):
    outfile = str(tmpdir) + '/out'
    sched = ImmediateRun()
    jobid = sched.submit(['false'], outfile)
    jobid = sched.submit(['echo', 'not echoed'], outfile, depends_on=[jobid])

    with open(outfile, 'r') as f:
        assert f.read().strip() == ''


def test_slurm_submit(monkeypatch, tmpdir):
    def check_output(cmd, universal_newlines=None):
        assert sorted(cmd) == sorted(
            ['sbatch', '-o', 'outfile', '-J', 'name', '0', '1'])
        return b'name 1'

    monkeypatch.setattr(subprocess, 'check_output', check_output)

    sched = Slurm(str(tmpdir))
    assert sched.submit(['0', '1'], 'outfile', name='name') == '1'


def test_slurm_submit_with_additional_args(monkeypatch, tmpdir):
    def check_output(cmd, universal_newlines=None):
        if cmd[0] == 'squeue':
            return '\n0 R name None\n'
        assert sorted(cmd) == sorted([
            'sbatch', '-t', '1h', '-o', 'outfile', '-J', 'name', '-d',
            'afterok:0', '0', '1'])
        return b'name 1'

    monkeypatch.setattr(subprocess, 'check_output', check_output)

    sched = Slurm(str(tmpdir))
    assert sched.submit(
        ['0', '1'], 'outfile', name='name', depends_on=['0'],
        scheduler_args={'timelimit': '1h'}) == '1'


def test_slurm_kill(monkeypatch, tmpdir):
    def check_call(cmd):
        assert cmd == ['scancel', '1']

    monkeypatch.setattr(subprocess, 'check_call', check_call)

    sched = Slurm(str(tmpdir))
    sched.kill('1')


def test_slurm_status(monkeypatch, tmpdir):
    def check_output(cmd, universal_newlines=None, stderr=None):
        assert cmd[:2] == ['squeue', '-u']
        assert cmd[3:5] == ['-h', '-o']
        assert cmd[5] == '%i %t %j %r' or cmd[5] == '%A %t %j %r'
        return '''id state name reason
1 R running None
2 PD pending Dependency
3 PD priority Priority
4_[0-2] PD array Priority
'''

    monkeypatch.setattr(subprocess, 'check_output', check_output)
    sched = Slurm(str(tmpdir))
    assert sorted(sched.get_jobs()) == ['1', '2', '3', '4_0', '4_1', '4_2']
    assert sched.get_status('1') == JobStatus('1', 'R', 'running')
    assert sched.get_status('2') == JobStatus('2', '*Q', 'pending')
    assert sched.get_status('3') == JobStatus('3', 'Q', 'priority')
    for i in range(3):
        job_id = '4_' + str(i)
        assert sched.get_status(job_id) == JobStatus(
            job_id, 'Q', 'array:' + str(i))


def test_slurm_submit_array(monkeypatch, tmpdir):
    class Popen(object):
        def __init__(self, cmd, stdin=None, stdout=None):
            assert sorted(cmd) == sorted(
                ['sbatch', '--array=0-2', '-o', 'outfile'])
            self.returncode = 0

        def communicate(self, input):
            assert input.decode('utf-8') == '#!' + sys.executable + '''
import os
import subprocess
import sys

task_id = os.environ['SLURM_ARRAY_TASK_ID']
sys.exit(subprocess.call([a.replace('%a', str(task_id)) for a in ['arg0', 'arg1']]))
'''
            return b'array 1', b''

    monkeypatch.setattr(subprocess, 'Popen', Popen)
    sched = Slurm(str(tmpdir))
    sched.submit_array(3, ['arg0', 'arg1'], 'outfile')
