import datetime
import unittest
from unittest import mock

from uit.pbs_script import PbsScript, NODE_TYPES


class TestPBSScript(unittest.TestCase):

    def setUp(self):
        self.pbs = PbsScript(name='test1', project_id='P001', num_nodes=5, processes_per_node=1, max_time="20:30:30")

    def tearDown(self):
        pass

    def test_init_name_value_error(self):
        self.assertRaises(ValueError, PbsScript, name=None, project_id='P001', num_nodes=5, processes_per_node=1,
                          max_time="20:30:30")

    def test_init_project_id_value_error(self):
        self.assertRaises(ValueError, PbsScript, name='test1', project_id=None, num_nodes=5, processes_per_node=1,
                          max_time="20:30:30")

    def test_init_max_time_value_error(self):
        self.assertRaises(ValueError, PbsScript, name='test2', project_id='P001', num_nodes=5, processes_per_node=10,
                          max_time=None)

    def test_repr(self):
        result = self.pbs.__repr__()
        self.assertEqual('<PbsScript name=test1>', result)

    @mock.patch('uit.pbs_script.PbsScript.render')
    def test_str(self, mock_render):
        self.pbs.__str__()
        mock_render.assert_called_once()

    def test_validate_system_value_error(self):
        self.pbs.system = "not a system"
        self.assertRaises(ValueError, self.pbs._validate_system)

    def test_validate_node_type_error(self):
        self.pbs.node_type = "not a node"
        self.assertRaises(ValueError, self.pbs._validate_node_type)

    def test_validate_processes_per_node_error(self):
        self.pbs.processes_per_node = "not a valid process per node"
        self.assertRaises(ValueError, self.pbs._validate_processes_per_node)

    def test_parse_time(self):
        time = datetime.timedelta(hours=5)
        result = self.pbs.parse_time(time)
        self.assertEqual(result, time)

    @mock.patch('uit.pbs_script.PbsDirective')
    def test_job_array_directives(self, mock_directive):
        self.pbs._array_indices = (1, 2)
        self.pbs.job_array_directives
        options = mock_directive.mock_calls[0].args[1]
        self.assertEqual(options, '1-2')

    @mock.patch('uit.pbs_script.PbsDirective')
    def test_job_array_directives_with_step(self, mock_directive):
        self.pbs._array_indices = (1, 2, 3)
        self.pbs.job_array_directives
        options = mock_directive.mock_calls[0].args[1]
        self.assertEqual(options, '1-2:3')

    def test_number_of_sub_jobs(self):
        self.pbs._array_indices = (1, 2)
        result = self.pbs.number_of_sub_jobs
        self.assertEqual(result, 3)

    def test_number_of_sub_jobs_array_not_set(self):
        result = self.pbs.number_of_sub_jobs
        self.assertEqual(result, None)

    def test_job_array_indices(self):
        self.pbs._array_indices = (1, 2)
        result = self.pbs.job_array_indices
        self.assertEqual(result, [1, 2])

    def test_set_directive(self):
        # Call the method
        self.pbs.set_directive('-l', 'application=other')

        # Get the list of namedtuple
        ret = self.pbs._optional_directives

        # Test the results
        self.assertEqual('-l', ret[0].directive)
        self.assertEqual('application=other', ret[0].options)

    def test_get_directive(self):
        self.pbs.set_directive('-A', 'C++')

        # Call the method
        res = self.pbs.get_directive('-A')

        # Test the result
        self.assertEqual(['C++'], res)

    def test_get_directive_first(self):
        self.pbs.set_directive('-A', 'Python')

        # Call the method
        res = self.pbs.get_directive('-A', first=True)

        # Test the result
        self.assertEqual('Python', res)

    def test_get_directive_default(self):

        # Call the method
        res = self.pbs.get_directive('-A', first=True, default='test')

        # Test the result
        self.assertEqual('test', res)

    def test_get_directives(self):
        self.pbs.set_directive('-l', 'application=other')
        self.pbs.set_directive('-o', 'stdout.log')

        # Call the method
        res = self.pbs.optional_directives

        # Test the result
        self.assertEqual('application=other', res[0].options)
        self.assertEqual('stdout.log', res[1].options)

    def test_module_use(self):
        self.pbs.module_use('path')
        self.assertIn('path', self.pbs._module_use)

    def test_set_environment_variable(self):
        self.pbs.set_environment_variable('key', 7)
        res = self.pbs.environment_variables['key']
        self.assertEqual(7, res)

    @mock.patch('uit.pbs_script.PbsScript._render_directive_list')
    @mock.patch('uit.pbs_script.PbsScript.job_array_directives', new_callable=mock.PropertyMock)
    def test_render_required_directives_block_not_None(self, mock_job_array_directives, mock_render):
        mock_job_array_directives.return_value = ['this is fake']
        self.pbs._array_indices = (1, 2, 3)
        self.pbs.render_required_directives_block()
        res = mock_render.call_args.args[1]
        self.assertIn('this is fake', res)

    def test_get_render_required_directives_block_for_narwhal_compute_node(self):
        pbs_script = PbsScript(name='test1', project_id='P001', num_nodes=5, processes_per_node=1, max_time="20:30:30",
                            node_type='compute', system='narwhal')
        res = pbs_script.render_required_directives_block()
        self.assertIn('#PBS -N ' + pbs_script.name, res)
        self.assertIn('#PBS -A ' + pbs_script.project_id, res)
        self.assertIn("#PBS -q " + pbs_script.queue, res)
        self.assertIn(f'#PBS -l select={pbs_script.num_nodes}:'
                      f'ncpus={NODE_TYPES["narwhal"]["compute"]}:'
                      f'mpiprocs={pbs_script.processes_per_node}', res)

    def test_get_render_required_directives_block_for_narwhal_gpu_node(self):
        pbs_script = PbsScript(name='test1', project_id='P001', num_nodes=5, processes_per_node=1, max_time="20:30:30",
                            node_type='gpu', system='narwhal')
        res = pbs_script.render_required_directives_block()
        self.assertIn(f'#PBS -l select={pbs_script.num_nodes}:ncpus={NODE_TYPES["narwhal"]["compute"]}:'
                      f'mpiprocs={pbs_script.processes_per_node}', res)

    def test_get_render_required_directives_block_for_narwhal_bigmem_node(self):
        pbs_script = PbsScript(name='test1', project_id='P001', num_nodes=5, processes_per_node=1, max_time="20:30:30",
                            node_type='bigmem', system='narwhal')
        res = pbs_script.render_required_directives_block()
        self.assertIn(f'#PBS -l select={pbs_script.num_nodes}:ncpus={NODE_TYPES["narwhal"]["bigmem"]}:'
                      f'mpiprocs={pbs_script.processes_per_node}', res)

    def test_get_render_required_directives_block_for_narwhal_transfer_node(self):
        pbs_script = PbsScript(name='test1', project_id='P001', num_nodes=5, processes_per_node=1, max_time="20:30:30",
                            node_type='transfer', system='narwhal')
        res = pbs_script.render_required_directives_block()
        self.assertIn('#PBS -l select=5:ncpus=1', res)

    def test_get_render_required_directives_block_for_carpenter_compute_node(self):
        pbs_script = PbsScript(name='test1', project_id='P001', num_nodes=5, processes_per_node=24, max_time="20:30:30",
                            node_type='compute', system='carpenter')
        expected = f'#PBS -l select={pbs_script.num_nodes}:ncpus={NODE_TYPES["carpenter"]["compute"]}:' \
                   f'mpiprocs={pbs_script.processes_per_node}'

        res = pbs_script.render_required_directives_block()
        self.assertIn(expected, res)

    def test_get_render_required_directives_block_for_carpenter_gpu_node(self):
        pbs_script = PbsScript(name='test1', project_id='P001', num_nodes=5, processes_per_node=32, max_time="20:30:30",
                            node_type='gpu', system='carpenter')
        expected = f'#PBS -l select={pbs_script.num_nodes}:ncpus={NODE_TYPES["carpenter"]["gpu"]}:' \
                   f'mpiprocs={pbs_script.processes_per_node}:ngpus=1'

        res = pbs_script.render_required_directives_block()
        self.assertIn(expected, res)

    def test_get_render_required_directives_block_for_carpenter_bigmem_node(self):
        pbs_script = PbsScript(name='test1', project_id='P001', num_nodes=5, processes_per_node=24, max_time="20:30:30",
                            node_type='bigmem', system='carpenter')
        expected = f'#PBS -l select={pbs_script.num_nodes}:ncpus={NODE_TYPES["carpenter"]["bigmem"]}:' \
                   f'mpiprocs={pbs_script.processes_per_node}:bigmem=1'

        res = pbs_script.render_required_directives_block()
        self.assertIn(expected, res)

    def test_get_render_required_directives_block_for_carpenter_transfer_node(self):
        pbs_script = PbsScript(name='test1', project_id='P001', num_nodes=5, processes_per_node=1, max_time="20:30:30",
                            node_type='transfer', system='carpenter')
        res = pbs_script.render_required_directives_block()
        self.assertIn('PBS -l select=5:ncpus=1', res)

    def test_get_render_optional_directives_block(self):
        self.pbs.set_directive('-A', 'ADH')
        self.pbs.set_directive('-o', 'stdout.log')

        res = self.pbs.render_optional_directives_block()

        self.assertIn('#PBS -A ADH', res)
        self.assertIn('#PBS -o stdout.log', res)

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

    def test_render_modules_block_opt_list_append(self):
        self.pbs._module_use.append('module use is...')
        res = self.pbs.render_modules_block()
        self.assertIn('module use is...', res)

    def test_render_job_dir_configuration(self):
        self.pbs.configure_job_dir = True
        res = self.pbs.render_job_dir_configuration()
        self.assertGreater(len(res), 0)

    def test_render(self):
        self.pbs.load_module('C++')

        self.pbs.unload_module('OpenGL')

        self.pbs.swap_module('Anaconda', 'OpenMP')

        self.pbs.set_directive('-o', 'stdout.log')

        self.pbs.set_directive('-T', 'OpenGL')

        # call the testing method
        render_str = self.pbs.render()

        self.assertIn("#PBS -N test1", render_str)
        self.assertIn("#PBS -A P001", render_str)
        self.assertIn("#PBS -N test1", render_str)
        self.assertIn("#PBS -q debug", render_str)
        self.assertIn("#PBS -l select=5:ncpus=192:mpiprocs=1", render_str)
        self.assertIn("#PBS -l walltime=20:30:30", render_str)
        self.assertIn('module load C++', render_str)
        self.assertIn('module unload OpenGL', render_str)
        self.assertIn('module swap Anaconda OpenMP', render_str)
        self.assertIn('#PBS -o stdout.log', render_str)
        self.assertIn('#PBS -T OpenGL', render_str)

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

    def test_init_node_type_value_error(self):
        self.assertRaises(ValueError, PbsScript, name='test1', project_id='P001', num_nodes=5,
                          processes_per_node=10, max_time=20, node_type='test_node')

    def test_init_system_value_error(self):
        self.assertRaises(ValueError, PbsScript, name='test1', project_id='P001', num_nodes=5,
                          processes_per_node=10, max_time=20, system='test_node')

    def test_init_jim_system_knl_node_value_error(self):
        self.assertRaises(ValueError, PbsScript, name='test1', project_id='P001', num_nodes=5,
                          processes_per_node=10, max_time=20, node_type='knl', system='jim')


if __name__ == '__main__':
    unittest.main()
