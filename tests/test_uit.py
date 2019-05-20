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
