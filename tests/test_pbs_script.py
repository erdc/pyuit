import unittest
import mock

from uit.pbs_script import PbsScript


class TestPBSScript(unittest.TestCase):

    def setUp(self):
        self.pbs = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=10, max_time=20,
                             A='ADH')

    def tearDown(self):
        pass

    def test_set_directive(self):
        # Call the method
        self.pbs.set_directive('-A', 'test@gmail.com')

        # Get the list of namedtuple
        ret = self.pbs._optional_directives

        # Test the results
        self.assertEqual('-A', ret[1].directive)
        self.assertEqual('test@gmail.com', ret[1].options)

    def test_get_directive(self):
        # Call the method
        res = self.pbs.get_directive('-A')

        # Test the result
        self.assertEqual('ADH', res)

    def test_get_directives(self):
        # Call the method
        res = self.pbs.get_directives()

        # Test the result
        self.assertEqual('ADH', res[0].options)

    def test_get_render_required_directives_block(self):
        res = self.pbs.render_required_directives_block()
        self.assertIn('#PBS -N ' + self.pbs.job_name, res)
        self.assertIn('#PBS -A ' + self.pbs.project_id, res)
        self.assertIn("#PBS -q " + self.pbs.queue, res)

    def test_get_render_optional_directives_block(self):
        res = self.pbs.render_optional_directives_block()

        self.assertIn('#PBS -A ADH', res)

    def test_load_module(self):
        # load anaconda module
        self.pbs.load_module('anaconda')

        # get all the modules
        ret = self.pbs.get_modules()

        self.assertEqual('anaconda', ret['load'])

    def test_unload_module(self):
        # unload C++ module
        self.pbs.unload_module('C++')

        # get all the modules
        ret = self.pbs.get_modules()

        self.assertEqual('C++', ret['unload'])

    def test_swap_module(self):
        # load modules
        self.pbs.load_module('OpenMP')
        self.pbs.swap_module('OpenMP', 'C++')

        # get all the modules
        ret = self.pbs.get_modules()

        self.assertEqual('C++', ret['load'])

    def test_render_modules_block(self):
        # load anaconda module
        self.pbs.load_module('anaconda')

        # unload C++ module
        self.pbs.unload_module('C++')

        # swap modules
        self.pbs.swap_module('anaconda', 'OpenMP')

        ret = self.pbs.render_modules_block()

        self.assertIn('module load OpenMP', ret)
        self.assertIn('module swap anaconda,OpenMP', ret)
        self.assertIn('module unload C++', ret)

    def test_render(self):
        # load anaconda module
        self.pbs.load_module('anaconda')

        # unload C++ module
        self.pbs.unload_module('C++')

        # swap modules
        self.pbs.swap_module('anaconda', 'OpenMP')

        # call the testing method
        render_str = self.pbs.render()

        self.assertIn("#PBS -N test1", render_str)
        self.assertIn("#PBS -A P001", render_str)
        self.assertIn("#PBS -N test1", render_str)
        self.assertIn("#PBS -q debug", render_str)
        self.assertIn("#PBS -l select=5:ncpus=36:mpiprocs=10", render_str)
        self.assertIn("#PBS -l walltime=20", render_str)
        self.assertIn('module load OpenMP', render_str)
        self.assertIn('module swap anaconda,OpenMP', render_str)
        self.assertIn('module unload C++', render_str)

    @mock.patch('uit.pbs_script.PbsScript.render')
    @mock.patch('io.open', new_callable=mock.mock_open)
    def test_write(self, mock_file, mock_render):
        mock_render.return_value = "render string"

        mock_write = mock.MagicMock()
        mock_file.return_value = mock_write

        # call the test method
        path = "root//home//testpath//psb.sh"
        self.pbs.write(path)

        mock_render.assert_called()

        mock_file.assert_called_with(path, 'w', newline='\n')

        mock_write.write.assert_called()


if __name__ == '__main__':
    unittest.main()

