import json
import os
import random
import threading
import tempfile
import uuid
from functools import wraps
from pathlib import PosixPath, Path
from urllib.parse import urljoin, urlencode  # noqa: F401

import dodcerts
import requests
import yaml
from flask import Flask, request, render_template_string
from werkzeug.serving import make_server

from .pbs_script import PbsScript
from .util import robust, HpcEnv

# optional dependency
try:
    import pandas as pd
    has_pandas = True
except ImportError:
    has_pandas = False

UIT_API_URL = 'https://www.uitplus.hpc.mil/uapi/'
DEFAULT_CA_FILE = dodcerts.where()
DEFAULT_CONFIG_FILE = os.path.join(os.path.expanduser('~'), '.uit')
HPC_SYSTEMS = ['topaz', 'onyx']

_auth_code = None
_token = None


class Client:
    """Provides a python abstraction for interacting with the UIT API.

    Attributes:
        client_id (str): ID issued by UIT to authorize this client.
        client_secret (str): Secret key associated with the client ID.
        config_file (str): Location of a config file containing, among other things, the Client ID and Secret Key.
        connected (bool): Flag indicating whether a connection has been made.
        scope (str):
        session_id (str): 16-digit Hexidecimal string identifying the current session. Auto-generated from urandom if
            not provided.
        token (str): Token from current UIT authorization.
    """
    def __init__(self, ca_file=None, config_file=None, client_id=None, client_secret=None, session_id=None, scope='UIT',
                 token=None):
        if ca_file is None:
            self.ca_file = DEFAULT_CA_FILE

        # Set arg-based attributes
        self.client_id = client_id
        self.client_secret = client_secret
        self.config_file = config_file
        self.scope = scope
        self.token = token

        # Set attribute defaults
        self.connected = False

        # Set private attribute defaults
        self._auth_code = None
        self._headers = None
        self._login_node = None
        self._login_nodes = None
        self._system = None
        self._systems = None
        self._uit_url = None
        self._uit_urls = None
        self._user = None
        self._userinfo = None
        self._username = None
        self._callback = None

        # Environmental variable cache
        self.env = HpcEnv(self)

        if self.config_file is None:
            self.config_file = DEFAULT_CONFIG_FILE

        if self.client_id is None:
            self.client_id = os.environ.get('UIT_ID')

        if self.client_secret is None:
            self.client_secret = os.environ.get('UIT_SECRET')

        if (self.client_id is None or self.client_secret is None) and self.token is None:
            with open(self.config_file, 'r') as f:
                self.config = yaml.safe_load(f)
                self.client_id = self.config.get('client_id')
                self.client_secret = self.config.get('client_secret')

        if self.client_id is None and self.client_secret is None and self.token is None:
            raise ValueError('Please provide either the client_id and client_secret as kwargs, environment vars '
                             '(UIT_ID, UIT_SECRET) or in auth config file: ' + self.config_file + ' OR provide an '
                             'access token as a kwarg.')

        if session_id is None:
            self.session_id = os.urandom(16).hex()

    def _ensure_connected(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not self.connected:
                raise RuntimeError(f'Must connect to a system before running "{func.__name__}". '
                                   f'Run "Client.connect" to connect to a system.')

            return func(self, *args, **kwargs)
        return wrapper

    @property
    def HOME(self):
        return PosixPath(self.env.HOME)

    @property
    def WORKDIR(self):
        return PosixPath(self.env.WORKDIR)

    @property
    def WORKDIR2(self):
        return PosixPath(self.env.WORKDIR2)

    @property
    def CENTER(self):
        return PosixPath(self.env.CENTER)

    @property
    def login_node(self):
        return self._login_node

    @property
    def login_nodes(self):
        return self._login_nodes

    @property
    def system(self):
        return self._system

    @property
    def systems(self):
        return self._systems

    @property
    def uit_url(self):
        return self._uit_url

    @property
    def uit_urls(self):
        return self._uit_urls

    @property
    def user(self):
        return self._user

    @property
    def userinfo(self):
        return self._userinfo

    @property
    def username(self):
        return self._username

    def _do_callback(self, *args):
        if self._callback:
            try:
                self._callback(*args)
            except Exception as e:
                print(e)

    def _as_df(self, data):
        if not has_pandas:
            raise RuntimeError('"as_df" cannot be set to True unless the Pandas module is installed.')
        return pd.DataFrame(data)

    def _resolve_path(self, path, default=None):
        path = path or default
        if isinstance(path, Path):
            return path.as_posix()
        return path

    def authenticate(self, notebook=None, width=800, height=800, callback=None):
        """Ensure we have an access token. Request one from the user if we do not.

        Args:
            notebook (bool): Flag to indicate we are running in a Jupyter Notebook.
            width (int): Width to make the notebook widget.
            height (int): Height to make the notebook widget.
        """
        self._callback = callback
        # check if we have available tokens/refresh tokens
        token = self.load_token()
        if token:
            print('access token available, no auth needed')
            self._do_callback(True)
            return

        # start flask server
        start_server(self.get_token, self.config_file)

        auth_url = self.get_auth_url()
        if notebook:
            import IPython
            return IPython.display.IFrame(auth_url, width, height)

        import webbrowser
        webbrowser.open(auth_url)

    def connect(self, system=None, login_node=None, exclude_login_nodes=()):
        """Connect this client to the UIT servers.

        Args:
            system (str): Specific system name to connect to. Cannot be used with login_node arg.
            login_node (str): Specific node name to connect to. Cannot be used with system arg.
            exclude_login_nodes (list): Nodes to exclude when selecting a login node. Ignored if login_node is
                specified.
        """
        # get access token from file
        # populate userinfo and header info
        token = self.load_token()
        if token is None:
            raise RuntimeError('No Valid Access Tokens Found, Please run authenticate() function and try again')
        self._headers = {'x-uit-auth-token': token}

        # retrieve user info
        self.get_userinfo()

        if all([system, login_node]) or not any([system, login_node]):
            raise ValueError('Please specify at least one of system or login_node and not both')

        if login_node is None:
            # pick random login node for system
            login_node = random.choice(list(set(self._login_nodes[system]) - set(exclude_login_nodes)))

        try:
            system = [sys for sys, nodes in self._login_nodes.items() if login_node in nodes][0]
        except Exception:
            raise ValueError('{} login node not found in available nodes'.format(login_node))

        self._login_node = login_node
        self._system = system
        self._username = self._userinfo['SYSTEMS'][self._system.upper()]['USERNAME']
        self._uit_url = self._uit_urls[login_node]
        self.connected = True

        print('Connected successfully to {} on {}'.format(login_node, system))

    def get_auth_url(self):
        """Generate Authorization URL with UIT Server.

        Example:
        https://www.uitplus.hpc.mil/uapi/authorize?client_id=e01012b4-ab2c-4d95-83b3-26600a13ee0c&scope=UIT&state=2342342

        Returns:
            str: Authorization URL.
        """
        url = urljoin(UIT_API_URL, 'authorize')

        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'state': self.session_id,
            'scope': self.scope
        }

        return url + '?' + urlencode(data)

    def get_token(self, auth_code=None):
        """Get token from the UIT server.

        Args:
            auth_code (str): The authentication code to use.
        """

        url = urljoin(UIT_API_URL, 'token')

        if auth_code:
            self._auth_code = auth_code

        # check for auth_code
        if self._auth_code is None:
            global _auth_code
            if _auth_code:
                self._auth_code = _auth_code
            else:
                raise RuntimeError('You must first authenticate to the UIT server and get a auth code. '
                                   'Then set the auth_code')

        # set up the data dictionary
        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'state': self.session_id,
            'scope': self.scope,
            'code': self._auth_code
        }

        token = requests.post(url, data=data, verify=self.ca_file)

        # check the response
        if token.status_code == requests.codes.ok:
            print('Access Token request succeeded.')
        else:
            raise IOError('Token request failed.')

        # assign token to global namespace
        global _token
        _token = token.json()['access_token']
        self._do_callback(True)

    def load_token(self):
        """Load a token from the global namespace.

        Returns:
            str: The access token.
        """

        if self.token is not None:
            return self.token

        global _token
        return _token

    def clear_token(self):
        """Remove token from global namespace."""
        # clear tokens saved in config file
        global _token
        _token = None
        self.token = None

    def get_userinfo(self):
        """Get User Info from the UIT server."""
        # request user info from UIT site
        data = requests.get(urljoin(UIT_API_URL, 'userinfo'), headers=self._headers, verify=self.ca_file).json()
        self._userinfo = data.get('userinfo')
        self._user = self._userinfo.get('USERNAME')
        self._systems = [sys.lower() for sys in self._userinfo['SYSTEMS'].keys()]
        self._login_nodes = {
            system:
                [node['HOSTNAME'].split('.')[0] for node in self._userinfo['SYSTEMS'][system.upper()]['LOGIN_NODES']]
                for system in self._systems
        }

        self._uit_urls = [
            [{node['HOSTNAME'].split('.')[0]: node['URLS']['UIT']}
             for node in self._userinfo['SYSTEMS'][system.upper()]['LOGIN_NODES']
             ] for system in self._systems
        ]
        self._uit_urls = {k: v for l in self._uit_urls for d in l for k, v in d.items()}

    def get_uit_url(self, login_node=None):
        """Generate the URL for a given login node

        Args:
            login_node (str): The login node to generate a URL for. If unspecified, a random one is chosen.

        Returns:

        """
        if login_node is None:
            if self._login_node is None:
                login_node = random.choice(self._login_nodes)
            else:
                login_node = self._login_node

        uit_url = self._uit_urls[login_node]
        # if login name provided find system
        username = self._userinfo['SYSTEMS'][self._system.upper()]['USERNAME']
        return uit_url, username

    @_ensure_connected
    @robust()
    def call(self, command, working_dir=None, full_response=False):
        """Execute commands on the HPC via the exec endpoint.

        Args:
            command (str): The command to run.
            working_dir (str, optional, default=None): Working directory from which to run the command.
                If None the the users $HOME directory will be used
            full_response(bool, default=False):
                If True return the full JSON response from the UIT+ server.

        Returns:
            str: stdout from the command.
        """
        # Need to do this manually to prevent recursive loop when resolving self.home
        working_dir = working_dir or self.HOME

        working_dir = self._resolve_path(working_dir)

        # construct the base options dictionary
        data = {'command': command, 'workingdir': working_dir}
        data = {'options': json.dumps(data)}
        r = requests.post(urljoin(self._uit_url, 'exec'), headers=self._headers, data=data, verify=self.ca_file)
        resp = r.json()
        if full_response:
            return resp
        if resp.get('success') == 'true':
            return resp.get('stdout') + resp.get('stderr')
        else:
            raise RuntimeError('UIT Command failed with response: ', resp)

    @_ensure_connected
    @robust()
    def put_file(self, local_path, remote_path=None):
        """Put files on the HPC via the putfile endpoint.

        Args:
            local_path (str): Local file to upload.
            remote_path (str): Remote file to upload to.

        Returns:
            str: API response as json
        """
        local_path = Path(local_path)
        assert local_path.is_file()
        filename = local_path.name
        remote_path = self._resolve_path(remote_path, self.HOME / filename)
        data = {'file': remote_path}
        data = {'options': json.dumps(data)}
        files = {'file': local_path.open(mode='rb')}
        r = requests.post(urljoin(self._uit_url, 'putfile'), headers=self._headers, data=data, files=files,
                          verify=self.ca_file)
        return r.json()

    @_ensure_connected
    @robust()
    def get_file(self, remote_path, local_path=None):
        """Get a file from the HPC via the getfile endpoint.

        Args:
            remote_path (str): Remote file to download.
            local_path (str): local file to download to.

        Returns:
            str: local_path
        """
        if local_path is None:
            remote_path = PosixPath(remote_path)
            filename = remote_path.name
            local_path = Path() / filename
        remote_path = self._resolve_path(remote_path)
        data = {'file': remote_path}
        data = {'options': json.dumps(data)}
        r = requests.post(urljoin(self._uit_url, 'getfile'), headers=self._headers, data=data, verify=self.ca_file,
                          stream=True)
        if r.status_code != 200:
            raise RuntimeError("UIT returned a non-success status code ({}). The file '{}' may not exist, or you may "
                               "not have permission to access it.".format(r.status_code, remote_path))
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
        return local_path

    @_ensure_connected
    @robust()
    def list_dir(self, path=None, parse=True, as_df=False):
        """Get a detailed directory listing from the HPC via the listdirectory endpoint.

        Args:
            path (str): Directory to list

        Returns:
            str: The API response as JSON
        """
        path = self._resolve_path(path, self.HOME)

        if not parse:
            return self.call(f'ls -la {path}')

        data = {'directory': path}
        data = {'options': json.dumps(data)}
        r = requests.post(urljoin(self._uit_url, 'listdirectory'), headers=self._headers, data=data,
                          verify=self.ca_file)
        result = r.json()

        if as_df and 'path' in result:
            ls = result['dirs']
            ls.extend(result['files'])
            return self._as_df(ls)
        return r.json()

    @_ensure_connected
    @robust()
    def show_usage(self, parse=True, as_df=False):
        """Get output from the `show_usage` command, which shows the subproject IDs

        Args:
            parse(bool, optional, default=True): return results parsed into a list of dicts rather than as a raw string.

        Returns:
            str: The API response
        """
        result = self.call('show_usage')
        if not parse:
            return result

        lines = result.splitlines()
        usage = [{
            'system': j[0],
            'subproject': j[1],
            'hours_allocated': j[2],
            'hours_used': j[3],
            'hours_remaining': j[4],
            'percent_remaining': j[5],
            'background_hours_used': j[6]
        } for j in [i.split() for i in lines[8:-1]]]

        if as_df:
            return self._as_df(usage)
        return usage

    @_ensure_connected
    @robust()
    def status(self, username=None, job_id=None, parse=True, as_df=False):
        username = username if username is not None else self.username

        cmd = 'qstat -H'
        if username:
            cmd += f' -u {username}'
        if job_id:
            cmd += f' {job_id}'

        result = self.call(cmd)
        if not parse:
            return result

        lines = result.split('--------------- -------- -------- ---------- ------ --- --- ------ ----- - -----\n')
        lines = lines[-1].splitlines()
        jobs = [dict(
            job_id=values[0],
            username=values[1],
            queue=values[2],
            jobname=values[3],
            session_id=values[4],
            nds=values[5],
            tsk=values[6],
            requested_memory=values[7],
            requested_time=values[8],
            status=values[9],
            elapsed_time=values[10],
        ) for values in [i.split() for i in lines]]

        if as_df:
            return self._as_df(jobs)
        return jobs

    @_ensure_connected
    def submit(self, pbs_script, working_dir=None, remote_name='run.pbs', local_temp_dir=None):
        """Submit a PBS Script.

        Args:
            pbs_script(PbsScript or str): PbsScript instance or string containing PBS script.
            working_dir(str): Path to working dir on supercomputer in which to run pbs script.
            remote_name(str): Custom name for pbs script on supercomputer. Defaults to "run.pbs".
            local_temp_dir(str): Path to local temporary directory if unable to write to os temp dir.

        Returns:
            bool: True if job submitted successfully.
        """
        working_dir = self._resolve_path(working_dir, self.WORKDIR)

        if local_temp_dir:
            pbs_script_path = os.path.join(local_temp_dir, str(uuid.uuid4()))
        else:
            pbs_script_path = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))

        # Write out PbsScript tempfile
        if isinstance(pbs_script, PbsScript):
            pbs_script.write(pbs_script_path)
        else:
            if Path(pbs_script).is_file():
                pbs_script_path = pbs_script
            else:
                pbs_script_text = pbs_script
                with open(pbs_script_path, 'w') as f:
                    f.write(pbs_script_text)

        # Transfer script to supercomputer using put_file()
        ret = self.put_file(pbs_script_path, os.path.join(working_dir, remote_name))

        if 'success' in ret and ret['success'] == 'false':
            raise RuntimeError('An exception occurred while submitting job script: {}'.format(ret['error']))

        # Submit the script using call() with qsub command
        try:
            job_id = self.call('qsub {}'.format(remote_name), working_dir)
        except RuntimeError as e:
            raise RuntimeError('An exception occurred while submitting job script: {}'.format(str(e)))

        # Clean up (remove temp files)
        os.remove(pbs_script_path)

        return job_id.strip()


############################################################
# Simple Flask Server to retrieve auth_code & access_token #
############################################################


class ServerThread(threading.Thread):
    def __init__(self, app):
        threading.Thread.__init__(self)
        self.srv = make_server('127.0.0.1', 5000, app)
        self.ctx = app.app_context()
        self.ctx.push()

    def run(self):
        self.srv.serve_forever()

    def shutdown(self):
        self.srv.shutdown()


def start_server(auth_func, config_file):
    app = Flask('get_uit_token')
    server = ServerThread(app)
    server.start()

    def shutdown_server():
        func = request.environ.get('werkzeug.server.shutdown')
        if func is None:
            raise RuntimeError('Not running with the Werkzeug Server')
        func()

    @app.route('/save_token', methods=['GET'])
    def save_token():
        """
        WebHook to parse auth_code from url and retrieve access_token
        """
        hidden = 'hidden'
        global _auth_code, _auth_url
        try:
            _auth_code = request.args.get('code')
            auth_func(auth_code=_auth_code)

            status = 'Succeeded'
            msg = ''

        except Exception as e:
            status = 'Failed'
            msg = str(e)
        finally:
            shutdown_server()

        html_template = f"""
        <!doctype html>
        <head>
            <title>UIT Authentication {status}</title>
        </head>
        <body>
            <h1 style="margin: 50px 182px;">UIT Authentication {status}</h1>
            <div {hidden}>{msg}</div>
        </body>

        """
        return render_template_string(html_template)

    return server
