import os


def init_virtualenv(venv=None):
    if os.environ.get('CI', False):
        return

    if venv is None:
        venv = os.environ.get('VIRTUAL_ENV', None)

    if venv is not None:
        activate_script = os.path.join(venv, 'bin', 'activate_this.py')
        if not os.path.exists(activate_script):
            activate_script = os.path.join(venv, 'Scripts', 'activate_this.py')

        with open(activate_script, 'r') as f:
            source = f.read()
        exec(
            compile(source, activate_script, 'exec'),
            dict(__file__=activate_script))
