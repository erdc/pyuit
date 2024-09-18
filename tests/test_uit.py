import unittest
from unittest import mock
from pathlib import PurePosixPath
from http.client import RemoteDisconnected
import requests

from uit import Client
from uit.exceptions import MaxRetriesError


class TestUIT(unittest.TestCase):

    def setUp(self):
        from uit import Client
        with mock.patch('uit.Client.get_userinfo') as _:
            self.client = Client(token='test_token')
            self.client.connected = True

    def tearDown(self):
        pass

    def test_optional_dependency_has_pandas(self):
        from uit import uit as uit_test
        from importlib import reload
        import builtins
        real_import = builtins.__import__
        try:
            def mock_import(name, *args):
                if name == 'pandas':
                    raise ModuleNotFoundError
                else:
                    return real_import(name, *args)

            builtins.__import__ = mock_import
            reload(uit_test)
        finally:
            builtins.__import__ = real_import
        self.assertEqual(uit_test.has_pandas, False)

    @mock.patch('uit.config.open')
    @mock.patch('uit.config.yaml.safe_load')
    def test_init_no_token(self, mock_yaml, _):
        mock_yaml.return_value = {
            'client_id': 'client_id',
            'client_secret': 'client_secret'
        }
        Client(config_file='test')

    @mock.patch('uit.config.open')
    @mock.patch('uit.config.yaml.safe_load')
    def test_init_no_credentials(self, mock_yaml, _):
        mock_yaml.return_value = {}
        self.assertRaises(ValueError, Client)

    def test_ensure_connected(self):
        self.client.connected = False
        self.assertRaises(
            RuntimeError, self.client.call, 'cmd'
        )

    @mock.patch('uit.Client')
    def test_HOME(self, _):
        self.client.env.HOME = 'HOME env'
        res = self.client.HOME
        self.assertEqual(PurePosixPath('HOME env'), res)

    @mock.patch('uit.Client')
    def test_WORKDIR(self, _):
        self.client.env.WORKDIR = 'WORKDIR'
        res = self.client.WORKDIR
        self.assertEqual(PurePosixPath('WORKDIR'), res)

    @mock.patch('uit.Client')
    def test_WORKDIR2(self, _):
        self.client.env.WORKDIR2 = 'WORKDIR2'
        res = self.client.WORKDIR2
        self.assertEqual(PurePosixPath('WORKDIR2'), res)

    @mock.patch('uit.Client')
    def test_CENTER(self, _):
        self.client.env.CENTER = 'CENTER'
        res = self.client.CENTER
        self.assertEqual(PurePosixPath('CENTER'), res)

    def test_token(self):
        self.assertEqual('test_token', self.client.token)

    def test_login_node(self):
        self.assertEqual(None, self.client.login_node)

    def test_login_nodes(self):
        self.assertEqual(None, self.client.login_nodes)

    def test_system(self):
        self.assertEqual(None, self.client.system)

    def test_systems(self):
        self.assertEqual(None, self.client.systems)

    def test_uit_url(self):
        self.assertEqual(None, self.client.uit_url)

    def test_uit_urls(self):
        self.assertEqual(None, self.client.uit_urls)

    def test_user(self):
        self.assertEqual(None, self.client.user)

    def test_userinfo(self):
        self.assertEqual(None, self.client.userinfo)

    def test_username(self):
        self.assertEqual(None, self.client.username)

    @mock.patch('uit.Client.call')
    @mock.patch('uit.Client.put_file')
    def test_submit(self, _, mock_call):
        mock_call.return_value = 'J001'

        ret = self.client.submit(pbs_script='test_script.sh', working_dir='\\test\\workdir')

        self.assertEqual('J001', ret)

    @mock.patch('uit.Client.call')
    @mock.patch('uit.Client.put_file')
    def test_submit_runtime_error(self, mock_put_file, mock_call):
        mock_put_file.return_value = {'success': 'false', 'error': 'test_error'}
        mock_call.return_value = 'J001'

        self.assertRaises(RuntimeError, self.client.submit, pbs_script='test_script.sh', working_dir='\\test\\workdir')

    @mock.patch('uit.Client.call')
    @mock.patch('uit.Client.put_file')
    def test_submit_call_error(self, _, mock_call):
        mock_call.side_effect = RuntimeError

        self.assertRaises(RuntimeError, self.client.submit, pbs_script='test_script.sh', working_dir='\\test\\workdir')

    @mock.patch('requests.post')
    def test_robust_dp_route_error(self, mock_post):
        """Test the @robust decorator for handling repeated DP Route errors"""
        error_text = ("DP Route error: Failed to start tunnel connection: Start Tunnel error: ChildProcessError: "
                      "Command failed: mk_uit_ssh_tunnel.sh")
        mock_post.side_effect = RuntimeError(error_text)
        self.assertRaises(MaxRetriesError, self.client.call, command='pwd', working_dir='.')

    @mock.patch('requests.post')
    def test_robust_connection_error(self, mock_post):
        """Test the @robust decorator for handling repeated Connection aborted errors"""
        error_text = ('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))
        mock_post.side_effect = requests.exceptions.ConnectionError(error_text)
        self.assertRaises(MaxRetriesError, self.client.call, command='pwd', working_dir='.')
