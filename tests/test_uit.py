import unittest
import mock

from uit.uit import Client


class TestUIT(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_client(self):
        client = Client(token='test_token')
        self.assertEqual('test_token', client.token)

    def test_load_token(self):
        client = Client(token='test_token')
        # call the method
        self.assertEqual('test_token', client.load_token())

    @mock.patch('builtins.open')
    @mock.patch('yaml.load')
    def test_load_token_config(self, mock_load, __):
        from datetime import datetime
        from datetime import timedelta
        client = Client(client_id='C001', client_secret='S001')
        mock_config = mock.MagicMock()
        mock_load.return_value = mock_config
        test_dt = datetime.now() + timedelta(days=1)
        test_dt_converted = test_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        mock_config.get.return_value = [{'access_token_expires_on': test_dt_converted, 'access_token': 'foo'}]
        # call the method
        self.assertEqual('foo', client.load_token())

    @mock.patch('builtins.open')
    @mock.patch('yaml.load')
    def test_load_token_config_none(self, mock_load, __):
        client = Client(client_id='C001', client_secret='S001')
        mock_config = mock.MagicMock()
        mock_load.return_value = mock_config
        mock_config.get.return_value = None
        # call the method
        self.assertIsNone(client.load_token())

    @mock.patch('uit.uit.Client.call')
    @mock.patch('uit.uit.Client.put_file')
    def test_submit(self, _, mock_call):
        client = Client(token='test_token')
        client.connected = True
        mock_call.return_value = 'J001'

        ret = client.submit(pbs_script='test_script.sh', working_dir='\\test\\workdir')

        self.assertEqual('J001', ret)

    @mock.patch('uit.uit.Client.call')
    @mock.patch('uit.uit.Client.put_file')
    def test_submit_runtime_error(self, mock_put_file, mock_call):
        client = Client(token='test_token')
        client.connected = True
        mock_put_file.return_value = {'success': 'false', 'error': 'test_error'}
        mock_call.return_value = 'J001'

        self.assertRaises(RuntimeError, client.submit, pbs_script='test_script.sh', working_dir='\\test\\workdir')

    @mock.patch('uit.uit.Client.call')
    @mock.patch('uit.uit.Client.put_file')
    def test_submit_call_error(self, _, mock_call):
        client = Client(token='test_token')
        client.connected = True
        mock_call.side_effect = RuntimeError

        self.assertRaises(RuntimeError, client.submit, pbs_script='test_script.sh', working_dir='\\test\\workdir')
