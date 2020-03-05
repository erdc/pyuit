import json
import os
import re
import random
import threading
import tempfile
import uuid
from functools import wraps
from itertools import chain
from pathlib import PurePosixPath, Path
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
HPC_SYSTEMS = ['jim', 'onyx']
QUEUES = ['standard', 'debug', 'transfer', 'background', 'HIE', 'high', 'frontier']

_auth_code = None
_server = None


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
                 token=None, port=5000):
        if ca_file is None:
            self.ca_file = DEFAULT_CA_FILE

        # Set arg-based attributes
        self.client_id = client_id
        self.client_secret = client_secret
        self.config_file = config_file
        self.session_id = session_id
        self.scope = scope
        self.token = token
        self.port = port

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
        self._available_modules = None

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
        return PurePosixPath(self.env.HOME)

    @property
    def WORKDIR(self):
        return PurePosixPath(self.env.WORKDIR)

    @property
    def WORKDIR2(self):
        return PurePosixPath(self.env.WORKDIR2)

    @property
    def CENTER(self):
        return PurePosixPath(self.env.CENTER)

    @property
    def headers(self):
        if self._headers is None:
            self._headers = {'x-uit-auth-token': self.token} if self.token else None
        return self._headers

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

    @property
    def available_modules(self):
        if self._available_modules is None:
            self.get_available_modules()
        return self._available_modules

    def _do_callback(self, *args):
        if self._callback:
            try:
                self._callback(*args)
            except Exception as e:
                print(e)

    def _as_df(self, data, columns=None):
        if not has_pandas:
            raise RuntimeError('"as_df" cannot be set to True unless the Pandas module is installed.')
        return pd.DataFrame.from_records(data, columns=columns)

    @staticmethod
    def _resolve_path(path, default=None):
        path = path or default
        if isinstance(path, Path):
            return path.as_posix()
        return path

    def authenticate(self, inline=None, width=800, height=800, callback=None):
        """Ensure we have an access token. Request one from the user if we do not.

        Args:
            inline (bool): Flag to show authentication site as an inline IFrame rather then opening in a browser tab.
            width (int): Width to make the inline widget.
            height (int): Height to make the inline widget.
            callback (func): Function to call once authentication process has happened. Should accept a boolean
                representing the authenticated status (i.e. True means authentication was successful).
        """
        self._callback = callback
        # check if we have available tokens/refresh tokens

        if self.token:
            print('access token available, no auth needed')
            self._do_callback(True)
            return

        # start flask server
        global _server
        if _server is not None:
            _server.auth_func = self.get_token
        else:
            _server = start_server(self.get_token, self.port)

        auth_url = self.get_auth_url()
        if inline:
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
        if self.token is None:
            raise RuntimeError('No Valid Access Tokens Found, Please run authenticate() function and try again')

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

        msg = 'Connected successfully to {} on {}'.format(login_node, system)
        return msg

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

        global _auth_code
        self._auth_code = auth_code or _auth_code

        # check for auth_code
        if self._auth_code is None:
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

        self.token = token.json()['access_token']
        self._do_callback(True)

    def get_userinfo(self):
        """Get User Info from the UIT server."""
        # request user info from UIT site
        data = requests.get(urljoin(UIT_API_URL, 'userinfo'), headers=self.headers, verify=self.ca_file).json()
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
    def call(self, command, working_dir=None, full_response=False, raise_on_error=True):
        """Execute commands on the HPC via the exec endpoint.

        Args:
            command (str): The command to run.
            working_dir (str, optional, default=None): Working directory from which to run the command.
                If None the the users $HOME directory will be used
            full_response(bool, default=False):
                If True return the full JSON response from the UIT+ server.
            raise_on_error(bool, default=True):
                If True then an error is raised if the call is not successful.

        Returns:
            str: stdout from the command.
        """
        # Need to do this manually to prevent recursive loop when resolving self.home
        working_dir = working_dir or self.HOME

        working_dir = self._resolve_path(working_dir)

        # construct the base options dictionary
        data = {'command': command, 'workingdir': working_dir}
        data = {'options': json.dumps(data, default=encode_pure_posix_path)}
        r = requests.post(urljoin(self._uit_url, 'exec'), headers=self.headers, data=data, verify=self.ca_file)
        resp = r.json()
        if full_response:
            return resp
        if resp.get('success') == 'true':
            return resp.get('stdout') + resp.get('stderr')
        elif raise_on_error:
            raise RuntimeError('UIT Command failed with response: ', resp)
        else:
            return 'ERROR!\n' + resp.get('stdout') + resp.get('stderr')

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
        data = {'options': json.dumps(data, default=encode_pure_posix_path)}
        files = {'file': local_path.open(mode='rb')}
        r = requests.post(urljoin(self._uit_url, 'putfile'), headers=self.headers, data=data, files=files,
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
            Path: local_path
        """
        remote_path = PurePosixPath(remote_path)
        local_path = Path(local_path) if local_path else Path() / remote_path.name
        remote_path = self._resolve_path(remote_path)
        data = {'file': remote_path}
        data = {'options': json.dumps(data, default=encode_pure_posix_path)}
        r = requests.post(urljoin(self._uit_url, 'getfile'), headers=self.headers, data=data, verify=self.ca_file,
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
        data = {'options': json.dumps(data, default=encode_pure_posix_path)}
        r = requests.post(urljoin(self._uit_url, 'listdirectory'), headers=self.headers, data=data,
                          verify=self.ca_file)
        result = r.json()

        if as_df and 'path' in result:
            ls = result['dirs']
            ls.extend(result['files'])
            columns = ('perms', 'type', 'owner', 'group', 'size', 'lastmodified', 'path', 'name')
            return self._as_df(ls, columns)
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

        columns = ['system', 'subproject', 'hours_allocated', 'hours_used',
                   'hours_remaining', 'percent_remaining', 'background_hours_used']
        delimiter = '========= ============= =========== =========== =========== ========= ==========\n'
        return self._parse_hpc_output(result, columns, as_df, delimiter=delimiter, remove_last_line=True)

    @_ensure_connected
    @robust()
    def status(self, job_id=None, username=None, full=False, with_historic=False, parse=True, as_df=False):
        username = username if username is not None else self.username

        cmd = 'qstat'

        if full:
            cmd += ' -f'
        elif username:
            cmd += f' -u {username}'

        if job_id:
            if isinstance(job_id, (tuple, list)):
                job_id = ' '.join(job_id)
            cmd += f' -x {job_id}'
            return self._process_status_command(cmd, parse=parse, full=full, as_df=as_df)
        else:
            # If no jobs are specified then
            result1 = self._process_status_command(cmd, parse=parse, full=full, as_df=as_df)
            if not with_historic:
                return result1
            else:
                cmd += ' -x'
                result2 = self._process_status_command(cmd, parse=parse, full=full, as_df=as_df)

                if not parse:
                    return result1, result2
                elif as_df:
                    return pd.concat((result1, result2))
                else:
                    result1.extend(result2)
                    return result1

    def _process_status_command(self, cmd, parse, full, as_df):
        result = self.call(cmd)

        if not parse:
            return result

        if full:
            result = self._parse_full_status(result)
            if as_df:
                return self._as_df(result).T
            else:
                return result

        delimiter = '--------------- -------- -------- ---------- ------ --- --- ------ ----- - -----\n'
        columns = ('job_id', 'username', 'queue', 'jobname', 'session_id', 'nds', 'tsk',
                   'requested_memory', 'requested_time', 'status', 'elapsed_time')

        return self._parse_hpc_output(result, columns, as_df, delimiter=delimiter)

    @staticmethod
    def _parse_full_status(status_str):
        clean_status_str = status_str.replace('\n\t', '').split('Job Id: ')[1:]
        statuses = dict()
        for status in clean_status_str:
            lines = status.splitlines()
            d = dict()
            for l in lines[1:-1]:
                try:
                    k, v = l.split('=', 1)
                    d[k.strip()] = v.strip()
                except ValueError:
                    print('ERROR', l)
            d['Variable_List'] = dict(kv.split('=') for kv in d.get('Variable_List').split(','))
            statuses[lines[0]] = d
        return statuses

    def _parse_hpc_output(self, output, columns, as_df, delimiter=None, remove_last_line=False):
        if delimiter is not None:
            lines = output.split(delimiter)[-1].splitlines()

        if remove_last_line:
            lines = lines[:-1]

        rows = [{k: v for k, v in list(zip(columns, i.split()))} for i in lines]

        if as_df:
            return self._as_df(rows, columns)
        return rows

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

        local_temp_dir = local_temp_dir or tempfile.gettempdir()

        pbs_script_path = os.path.join(local_temp_dir, str(uuid.uuid4()))

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
            job_id = self.call(f'qsub {remote_name}', working_dir=working_dir)
        except RuntimeError as e:
            raise RuntimeError('An exception occurred while submitting job script: {}'.format(str(e)))

        # Clean up (remove temp files)
        os.remove(pbs_script_path)

        return job_id.strip()

    @_ensure_connected
    def get_queues(self):
        output = self.call('qstat -Q')
        standard_queues = [] if self.system == 'jim' else QUEUES
        other_queues = set([i.split()[0] for i in output.splitlines()][2:]) - set(standard_queues)
        all_queues = standard_queues + sorted([q for q in other_queues if '_' not in q])
        return all_queues

    @_ensure_connected
    def get_available_modules(self, flatten=False):
        output = self.call('module avail')
        output = re.sub('.*:ERROR:.*', '', output)
        sections = re.split('-+ (.*) -+', output)[1:]
        self._available_modules = {a: b.split() for a, b in zip(sections[::2], sections[1::2])}

        if flatten:
            return sorted(chain.from_iterable(self._available_modules.values()))
        return self._available_modules

    @_ensure_connected
    def get_loaded_modules(self):
        output = self.call('module list')
        output = re.sub('.*:ERROR:.*', '', output)
        return re.split('\n?\s*\d+\)\s*', output[:-1])[1:]

############################################################
# Simple Flask Server to retrieve auth_code & access_token #
############################################################


class ServerThread(threading.Thread):
    def __init__(self, app, port, auth_func):
        threading.Thread.__init__(self)
        self.srv = make_server('127.0.0.1', port, app)
        self.auth_func = auth_func
        self.ctx = app.app_context()
        self.ctx.push()

    def run(self):
        self.srv.serve_forever()

    def shutdown(self):
        self.srv.shutdown()


def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()


def start_server(auth_func, port=5000):
    app = Flask('get_uit_token')
    server = ServerThread(app, port, auth_func)
    server.start()

    @app.route('/save_token', methods=['GET'])
    def save_token():
        """
        WebHook to parse auth_code from url and retrieve access_token
        """
        hidden = 'hidden'
        global _auth_code
        try:
            _auth_code = request.args.get('code')
            server.auth_func(auth_code=_auth_code)

            status = 'Succeeded'
            msg = ''

        except Exception as e:
            status = 'Failed'
            msg = str(e)

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


def encode_pure_posix_path(obj):
    if isinstance(obj, PurePosixPath):
        return obj.as_posix()
    else:
        raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')
