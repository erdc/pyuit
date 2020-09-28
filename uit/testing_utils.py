from uit import Client
from pathlib import Path
from subprocess import run


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
        self._uit_urls = {k: v for l in self._uit_urls for d in l for k, v in d.items()}

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

    def call(self, *args, **kwargs):
        try:
            completed_process = run(args[0].split(), capture_output=True)
            return completed_process.stdout.decode('utf-8')
        except:
            return ''

    def put_file(self, *args, **kwargs):
        pass

    def submit(self, *args, **kwargs):
        pass

    def list_dir(self, path=None, parse=True, as_df=False):
        p = Path(path)
        dirs = [{'name': x.name, 'path': str(x)} for x in p.iterdir() if x.is_dir()]
        files = [{'name': x.name, 'path': str(x)} for x in p.iterdir() if x.is_file()]
        return {'dirs': dirs, 'files': files}


mock_client = MockClient()
