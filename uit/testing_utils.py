from uit import Client, PbsScript, PbsJob, PbsArrayJob
from pathlib import Path
from subprocess import run
import shutil


class MockClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(token='mock_token')
        self.connect('onyx')

    @property
    def WORKDIR(self):
        return Path().resolve()

    @property
    def HOME(self):
        return Path().resolve()

    def get_userinfo(self):
        """Get User Info from the UIT server."""
        # request user info from UIT site
        self._userinfo = {
            'USERNAME': 'mock_user',
            'SYSTEMS': {
                'ONYX': {
                    'LOGIN_NODES': [
                        {
                            'HOSTNAME': 'onyx01',
                            'USERNAME': 'mock_user',
                            'URLS': {'UIT': 'onyx01@mock.gov'}
                        },
                    ]
                }
            }
        }
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
        self._uit_urls = {k: v for l in self._uit_urls for d in l for k, v in d.items()}  # noqa: E741

    def connect(self, system, **kwargs):
        self._system = system
        self._login_node = f'{system}01'
        self._system = system
        self._username = 'mock_user'
        self._uit_url = self._uit_urls[self.login_node]
        self.connected = True
        return 'Mock Success'

    def show_usage(self):
        columns = ['system', 'subproject', 'hours_allocated', 'hours_used',
                   'hours_remaining', 'percent_remaining', 'background_hours_used']
        return [{k: 'mock' for k in columns}]

    def call(self, command, *args, full_response=False, working_dir=None, **kwargs):
        cmd_args = command.split()
        try:
            completed_process = run(command, capture_output=True, cwd=working_dir, shell=True)
            stdout = completed_process.stdout.decode('utf-8')
            stderr = completed_process.stderr.decode('utf-8')
            if full_response:
                return {'stdout': stdout, 'stderr': stderr}
            return stdout
        except:  # noqa: E722
            try:  # For Windows
                if cmd_args[0] == 'cat':
                    with open(cmd_args[1]) as f:
                        return f.read()
            except:  # noqa: E722
                pass
            return ''

    def get_file(self, *args, **kwargs):
        pass

    def put_file(self, local_path, remote_path=None):
        shutil.copy(local_path, remote_path)

    def submit(self, *args, **kwargs):
        pass

    def list_dir(self, path=None, parse=True, as_df=False):
        p = Path(path)
        try:
            dirs = [{'name': x.name, 'path': str(x)} for x in p.iterdir() if x.is_dir()]
            files = [{'name': x.name, 'path': str(x)} for x in p.iterdir() if x.is_file()]
            return {'dirs': dirs, 'files': files}
        except NotADirectoryError:
            return {'success': 'false', 'error': 'File supplied is not a directory.'}


mock_client = MockClient()

mock_script = PbsScript(
    name='mock_script',
    project_id='mock_project_id',
    num_nodes=1,
    processes_per_node=44,
    max_time='00:00:01'
)

mock_job = PbsJob(mock_script, client=mock_client, label='mock')

mock_array_script = PbsScript(
    name='mock_script',
    project_id='mock_project_id',
    num_nodes=1,
    processes_per_node=44,
    max_time='00:00:01',
    array_indices=(0, 2)
)

mock_array_job = PbsArrayJob(
    script=mock_array_script,
    client=mock_client,
    label='mock',
)
mock_array_job._job_id = '0[]'
mock_array_job._remote_workspace_id = 'mock_workspace'
