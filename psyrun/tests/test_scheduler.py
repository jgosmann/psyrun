from psyrun.scheduler import ImmediateRun


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
