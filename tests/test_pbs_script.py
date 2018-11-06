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
        self.pbs.set_directive('-J', 'OepnGL')

        # Get the list of namedtuple
        ret = self.pbs._optional_directives

        # Test the results
        self.assertEqual('-J', ret[1].directive)
        self.assertEqual('OepnGL', ret[1].options)

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

    def test_get_render_required_directives_block_for_topaz_compute_node(self):
        res = self.pbs.render_required_directives_block()
        self.assertIn('#PBS -N ' + self.pbs.job_name, res)
        self.assertIn('#PBS -A ' + self.pbs.project_id, res)
        self.assertIn("#PBS -q " + self.pbs.queue, res)
        self.assertIn('#PBS -l select={}:ncpus=36:mpiprocs={}'.format(self.pbs.num_nodes, self.pbs.processes_per_node),
                      res)

    def test_get_render_required_directives_block_for_topaz_gpu_node(self):
        pbs_gpu = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=10, max_time=20,
                            node_type='gpu', A='ADH')
        res = pbs_gpu.render_required_directives_block()
        self.assertIn('#PBS -l select={}:ncpus=28:mpiprocs={}'.format(pbs_gpu.num_nodes, pbs_gpu.processes_per_node),
                      res)

    def test_get_render_required_directives_block_for_topaz_bigmem_node(self):
        pbs_gpu = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=10, max_time=20,
                            node_type='bigmem', A='ADH')
        res = pbs_gpu.render_required_directives_block()
        self.assertIn('#PBS -l select={}:ncpus=32:mpiprocs={}'.format(pbs_gpu.num_nodes, pbs_gpu.processes_per_node),
                      res)

    def test_get_render_required_directives_block_for_topaz_transfer_node(self):
        pbs_gpu = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=10, max_time=20,
                            node_type='transfer', A='ADH')
        res = pbs_gpu.render_required_directives_block()
        self.assertIn('#PBS -l select=1:ncpus=1', res)

    def test_get_render_required_directives_block_for_onyx_compute_node(self):
        pbs_gpu = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=11, max_time=20,
                            node_type='compute', system='onyx', A='ADH')
        expected = '#PBS -l select={}:ncpus={}:mpiprocs={}'.format(pbs_gpu.num_nodes, 44, pbs_gpu.processes_per_node)

        res = pbs_gpu.render_required_directives_block()
        self.assertIn(expected, res)

    def test_get_render_required_directives_block_for_onyx_compute_node_value_error(self):
        pbs_gpu = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=15, max_time=20,
                            node_type='compute', system='onyx', A='ADH')

        self.assertRaises(ValueError, pbs_gpu.render_required_directives_block)

    def test_get_render_required_directives_block_for_onyx_gpu_node(self):
        pbs_gpu = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=11, max_time=20,
                            node_type='gpu', system='onyx', A='ADH')
        expected = '#PBS -l select={}:ncpus={}:mpiprocs={}2:ngpus=1'.format(pbs_gpu.num_nodes, 22, pbs_gpu.processes_per_node)

        res = pbs_gpu.render_required_directives_block()
        self.assertIn(expected, res)

    def test_get_render_required_directives_block_for_onyx_gpu_node_value_error(self):
        pbs_gpu = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=15, max_time=20,
                            node_type='gpu', system='onyx', A='ADH')

        self.assertRaises(ValueError, pbs_gpu.render_required_directives_block)

    def test_get_render_required_directives_block_for_onyx_bigmem_node(self):
        pbs_gpu = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=11, max_time=20,
                            node_type='bigmem', system='onyx', A='ADH')
        expected = '#PBS -l select={}:ncpus={}:mpiprocs={}:bigmem=1'.format(pbs_gpu.num_nodes, 44,
                                                                            pbs_gpu.processes_per_node)

        res = pbs_gpu.render_required_directives_block()
        self.assertIn(expected, res)

    def test_get_render_required_directives_block_for_onyx_bigmem_node_value_error(self):
        pbs_gpu = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=15, max_time=20,
                            node_type='bigmem', system='onyx', A='ADH')

        self.assertRaises(ValueError, pbs_gpu.render_required_directives_block)

    def test_get_render_required_directives_block_for_onyx_transfer_node(self):
        pbs_gpu = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=11, max_time=20,
                            node_type='transfer', system='onyx', A='ADH')
        res = pbs_gpu.render_required_directives_block()
        self.assertIn('PBS -l select=1:ncpus=1', res)

    def test_get_render_required_directives_block_for_onyx_knl_node(self):
        pbs_gpu = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=16, max_time=20,
                            node_type='knl', system='onyx', A='ADH')
        expected = '#PBS -l select={}:ncpus={}:mpiprocs={}:nmics=1'.format(pbs_gpu.num_nodes, 64,
                                                                           pbs_gpu.processes_per_node)

        res = pbs_gpu.render_required_directives_block()
        self.assertIn(expected, res)

    def test_get_render_required_directives_block_for_onyx_knl_node_value_error(self):
        pbs_gpu = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=15, max_time=20,
                            node_type='knl', system='onyx', A='ADH')

        self.assertRaises(ValueError, pbs_gpu.render_required_directives_block)

    def test_get_render_optional_directives_block(self):
        self.pbs.set_directive('-J', 'OpenMP')

        res = self.pbs.render_optional_directives_block()

        self.assertIn('#PBS -A ADH', res)
        self.assertIn('#PBS -J OpenMP', res)

    def test_load_module(self):
        # load anaconda module
        self.pbs.load_module('anaconda')

        # get all the modules
        ret = self.pbs.get_modules()

        self.assertEqual('load', ret['anaconda'])

    def test_unload_module(self):
        # unload anaconda module
        self.pbs.unload_module('anaconda')

        # get all the modules
        ret = self.pbs.get_modules()

        self.assertEqual('unload', ret['anaconda'])

    def test_swap_module(self):
        # load modules
        self.pbs.swap_module('OpenMP', 'C++')

        expected = {'OpenMP': 'C++'}

        # get all the modules
        ret = self.pbs.get_modules()

        self.assertDictEqual(expected, ret)

    def test_render_modules_block(self):
        self.pbs.load_module('C++')
        self.pbs.unload_module('OpenGL')
        self.pbs.swap_module('Anaconda', 'OpenMP')

        ret = self.pbs.render_modules_block()

        self.assertIn('module load C++', ret)
        self.assertIn('module unload OpenGL', ret)
        self.assertIn('module swap Anaconda OpenMP', ret)

    def test_render(self):

        self.pbs.load_module('C++')

        self.pbs.unload_module('OpenGL')

        self.pbs.swap_module('Anaconda', 'OpenMP')

        # call the testing method
        render_str = self.pbs.render()

        self.assertIn("#PBS -N test1", render_str)
        self.assertIn("#PBS -A P001", render_str)
        self.assertIn("#PBS -N test1", render_str)
        self.assertIn("#PBS -q debug", render_str)
        self.assertIn("#PBS -l select=5:ncpus=36:mpiprocs=10", render_str)
        self.assertIn("#PBS -l walltime=20", render_str)
        self.assertIn('module load C++', render_str)
        self.assertIn('module unload OpenGL', render_str)
        self.assertIn('module swap Anaconda OpenMP', render_str)

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

        call_args = mock_write.write.call_args_list

        mock_write.write.assert_called()

        self.assertEqual('render string', call_args[0][0][0])

    def test_init_node_type_value_error(self):
        self.assertRaises(ValueError, PbsScript, job_name='test1', project_id='P001', num_nodes=5,
                          processes_per_node=10, max_time=20, node_type='test_node', A='ADH')

    def test_init_system_value_error(self):
        self.assertRaises(ValueError, PbsScript, job_name='test1', project_id='P001', num_nodes=5,
                          processes_per_node=10, max_time=20, system='test_node', A='ADH')

    def test_init_topaz_system_knl_node_value_error(self):
        self.assertRaises(ValueError, PbsScript, job_name='test1', project_id='P001', num_nodes=5,
                          processes_per_node=10, max_time=20, node_type='knl', system='topaz', A='ADH')


if __name__ == '__main__':
    unittest.main()

