from __future__ import absolute_import
from __future__ import print_function

import uuid
import os
import json
import requests
import io
import yaml
from datetime import datetime
import threading
import random
import tempfile
from flask import Flask, request, render_template_string
from uit.pbs_script import PbsScript
from uit.util import robust


try:
    # Python 3
    # noinspection PyCompatibility
    from urllib.parse import urljoin, urlparse, parse_qs, urlencode  # noqa: F401
except ImportError:
    # Python 2
    from urlparse import urljoin, urlparse, parse_qs, urlencode  # noqa: F401

from werkzeug.serving import make_server

UIT_API_URL = 'https://www.uitplus.hpc.mil/uapi/'
pkg_dir, _ = os.path.split(__file__)
DEFAULT_CA_FILE = os.path.join(pkg_dir, "data", "DoD_CAs.pem")
DEFAULT_CONFIG_FILE = os.path.join(os.path.expanduser('~'), '.uit')

_auth_code = None


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

        if self.config_file is None:
            self.config_file = DEFAULT_CONFIG_FILE

        if self.client_id is None:
            self.client_id = os.environ.get('UIT_ID')

        if self.client_secret is None:
            self.client_secret = os.environ.get('UIT_SECRET')

        if (self.client_id is None or self.client_secret is None) and self.token is None:
            with open(self.config_file, 'r') as f:
                self.config = yaml.load(f)
                self.client_id = self.config.get('client_id')
                self.client_secret = self.config.get('client_secret')

        if self.client_id is None and self.client_secret is None and self.token is None:
            raise ValueError('Please provide either the client_id and client_secret as kwargs, environment vars '
                             '(UIT_ID, UIT_SECRET) or in auth config file: ' + self.config_file + ' OR provide an '
                             'access token as a kwarg.')

        if session_id is None:
            self.session_id = os.urandom(16).hex()

    def authenticate(self, notebook=None, width=800, height=800):
        """Ensure we have an access token. Request one from the user if we do not.

        Args:
            notebook (bool): Flag to indicate we are running in a Jupyter Notebook.
            width (int): Width to make the notebook widget.
            height (int): Height to make the notebook widget.
        """
        # check if we have available tokens/refresh tokens
        token = self.load_token()
        if token:
            print('access token available, no auth needed')
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
            raise ValueError('Please specify at one of system or login_node and not both')

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
        https://uit.erdc.dren.mil/uapi/authorize?client_id=e01012b4-ab2c-4d95-83b3-26600a13ee0c&scope=UIT&state=2342342

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
        """Get tokens from the UIT server.

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
                # stop_server()  # TODO: Check if this is still needed
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

        # get the token
        token = requests.post(url, data=data, verify=self.ca_file)

        # check the response
        if token.status_code == requests.codes.ok:
            print('Access Token request succeeded.')
        else:
            raise IOError('Token request failed.')

        token = token.json()

        # python does not appear to like the trailing 'Z' on the ISO formatted
        #  expiration dates.  we slice the last char off.
        token['access_token_expires_on'] = token['access_token_expires_on'][:-1]
        token['refresh_token_expires_on'] = token['refresh_token_expires_on'][:-1]
        # save token to config file
        self.save_token(token)

    def load_token(self):
        """Load a token from the config file.

        Returns:
            str: The access token.
        """
        if self.token is not None:
            return self.token
        with open(self.config_file, 'r') as f:
            config = yaml.load(f)
            tokens = config.get('tokens')
            if tokens is None:
                return None

            now = datetime.now()
            for token in tokens:
                expires_on = datetime.strptime(token['access_token_expires_on'], "%Y-%m-%dT%H:%M:%S.%f")
                if expires_on > now:
                    return token['access_token']

    def clear_tokens(self):
        """Remove all tokens from config file."""
        # clear tokens saved in config file
        with open(self.config_file, 'r') as f:
            config = yaml.load(f)

        config.pop('tokens', None)
        with open(self.config_file, 'w') as f:
            yaml.dump(config, f)

    def save_token(self, token):
        """Save a token to the config file.

        Args:
            token (str): Token to save.
        """
        with open(self.config_file, 'r') as f:
            config = yaml.load(f)

        if config.get('tokens') is None:
            config['tokens'] = []

        config['tokens'].append(token)
        with open(self.config_file, 'w') as f:
            yaml.dump(config, f)

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

    @robust()
    def call(self, command, work_dir):
        """Execute commands on the HPC via the exec endpoint.

        Args:
           command (str): The command to run.
           work_dir (str): Working directory from which to run the command.

        Returns:
            str: stdout from the command.
        """
        # construct the base options dictionary
        data = {'command': command, 'workingdir': work_dir}
        data = {'options': json.dumps(data)}
        r = requests.post(urljoin(self._uit_url, 'exec'), headers=self._headers, data=data, verify=self.ca_file)
        resp = r.json()
        if resp.get('success') == 'true':
            return resp.get('stdout')
        else:
            raise RuntimeError('UIT Command failed with response: ', resp)

    @robust()
    def put_file(self, local_path, remote_path):
        """Put files on the HPC via the putfile endpoint.

        Args:
            local_path (str): Local file to upload.
            remote_path (str): Remote file to upload to.

        Returns:
            str: API response as json
        """
        data = {'file': remote_path}
        data = {'options': json.dumps(data)}
        files = {'file': open(local_path, 'rb')}
        r = requests.post(urljoin(self._uit_url, 'putfile'), headers=self._headers, data=data, files=files,
                          verify=self.ca_file)
        return r.json()

    @robust()
    def get_file(self, remote_path, local_path):
        """Get a file from the HPC via the getfile endpoint.

        Args:
            remote_path (str): Remote file to download.
            local_path (str): local file to download to.

        Returns:
            str: local_path
        """
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

    @robust()
    def list_dir(self, path=None, system=None, login_node=None):  # TODO: Ensure we can remove unused 'system' arg
        """Get a detailed directory listing from the HPC via the listdirectory endpoint.

        Args:
            path (str): Directory to list
            system (str): System to connect to. (Unused)
            login_node: Node to connect to. If None, a random one is chosen.

        Returns:
            str: The API response as JSON
        """
        uit_url, username = self.get_uit_url(login_node=login_node)
        if path is None:
            path = os.path.join('/p/home', username)

        data = {'directory': path}
        data = {'options': json.dumps(data)}
        r = requests.post(urljoin(self._uit_url, 'listdirectory'), headers=self._headers, data=data,
                          verify=self.ca_file)
        return r.json()

    # TODO: Remove this and use the PbsScript class
    def create_submit_script(self, hpc_subproject, nodes, project_name, job_type='adh', queue='standard',
                             walltime='01:00:00', path=None, filename='submit_pbs', job_name='default',
                             output_file='adh.out', email=None):
        """Method to create a simple PBS submit script for ERDC HPC systems (currently only supporting Topaz and AdH).

        Args:
            hpc_subproject (str): HPC System subproject (e.g. ERDCV00898ADH)
            nodes (int): Total number of nodes to run on (not processors)
            project_name (str): Root filename for AdH simulation (without extension)
            job_type (str): Type of job being submitted (default='adh'). AdH is the only type supported currently.
            queue (str): Queue in which to submit (default='standard')
            walltime (str): Walltime for this job (default='01:00:00')
            path (str): Directory in which to save the submit script (default=cwd)
            filename (str): Filename for the submit script (default='submit_pbs')
            job_name (str): Job name in the PBS system (default='default')
            output_file (str): Output filename for the run output (default='adh.out')
            email (str): Email to send notifications to (default=None)
        """

        # if no path given, use cwd
        if path is None:
            path = os.getcwd()

        # if TOPAZ
        if self._system.upper() == 'TOPAZ':
            # set the number of mpi procs per node
            mpiprocs = 36
            # calculate the number of procs
            procs = mpiprocs * nodes

            if job_type == 'adh':
                # set the path to the executable
                exe_path = '$PROJECTS_HOME/AdH_SW/adh_V5_BETA'
                # create the run string
                launch_string = 'mpiexec_mpt -np ' + str(procs) + ' ' + \
                                exe_path + ' ' + project_name + ' > ' + output_file
            else:
                raise IOError('Job type other than AdH is not supported.')

        else:
            raise IOError('System {} not recognized. Cannot create submit script.'.format(self._system))

        # Open the file
        full_path = os.path.join(path, filename)
        outfile = io.open(full_path, 'w', newline='\n')

        outfile.write('#!/bin/bash \n')
        outfile.write('##Required PBS Directives -------------------------------------- \n')
        outfile.write('#PBS -A {} \n'.format(hpc_subproject))
        outfile.write('#PBS -q {} \n'.format(queue))
        outfile.write('#PBS -l select={}:ncpus={}:mpiprocs={} \n'.format(nodes, mpiprocs, mpiprocs))
        outfile.write('#PBS -l walltime={} \n'.format(walltime))
        outfile.write('#PBS -j oe \n')
        outfile.write('#PBS -N {} \n'.format(job_name))
        if email:
            outfile.write('#PBS -m e \n')
            outfile.write('#PBS -M {} \n'.format(str(email)))
        outfile.write(' \n')
        outfile.write('## Execution Block ---------------------------------------------- \n')
        outfile.write('# Environment Setup \n')
        outfile.write('# cd to your scratch directory in /work \n')
        outfile.write('cd $PBS_O_WORKDIR \n')
        outfile.write(' \n')
        outfile.write('## Launching ----------------------------------------------------- \n')
        outfile.write('{} \n'.format(launch_string))
        outfile.write(' \n')
        outfile.write('END \n')
        outfile.write(' \n')

    def submit(self, pbs_script, working_dir, remote_name='run.pbs', local_temp_dir=''):
        """Submit a PBS Script.

        Args:
            pbs_script(PbsScript or str): PbsScript instance or string containing PBS script.
            working_dir(str): Path to working dir on supercomputer in which to run pbs script.
            remote_name(str): Custom name for pbs script on supercomputer. Defaults to "run.pbs".
            local_temp_dir(str): Path to local temporary directory if unable to write to os temp dir.

        Returns:
            bool: True if job submitted successfully.
        """
        if not self.connected:
            raise RuntimeError('Must connect to system before submitting.')

        if not local_temp_dir:
            pbs_script_path = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))
        else:
            pbs_script_path = os.path.join(local_temp_dir, str(uuid.uuid4()))

        # Write out PbsScript tempfile
        if isinstance(pbs_script, PbsScript):
            pbs_script.write(pbs_script_path)
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

        global _auth_code, _auth_url
        _auth_code = request.args.get('code')
        auth_func(auth_code=_auth_code)

        context = {'auth_code': _auth_code, 'config_file': config_file}

        html_template = """
        <!doctype html>
        <title>UIT Authentication Succeeded</title>
        <h1>UIT Authentication Succeeded!</h1>
        <h2>Successfully retrieved Authentication Code: {{ auth_code }}</h2>
        <h2>Access Token Saved to {{ config_file }}</h2>
        """
        shutdown_server()
        return render_template_string(html_template, **context)
