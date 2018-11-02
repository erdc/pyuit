import unittest

from uit.pbs_script import PbsScript


class TestPBSScript(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_set_directive(self):
        pbs = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=10, max_time=20,
                        m='be')

        pbs.set_directive('-A', 'test@gmail.com')

        ret = pbs._optional_directives

        self.assertIn('-A', ret[1])

    def test_get_directive(self):
        pbs = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=10, max_time=20,
                        m='be')

        res = pbs.get_directive('-m')

        self.assertEqual('be', res)

    def test_get_directives(self):
        pbs = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=10, max_time=20,
                        m='be')

        res = pbs.get_directives()

        self.assertEqual('be', res[0].options)

    def test_get_render_required_directives_block(self):
        pbs = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=10, max_time=20,
                        m='be')
        res = pbs.render_required_directives_block()
        self.assertIn('#PBS -N ' + pbs.job_name, res)
        self.assertIn('#PBS -A ' + pbs.project_id, res)
        self.assertIn("#PBS -q " + pbs.queue, res)

    def test_get_render_optional_directives_block(self):
        pbs = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=10, max_time=20,
                        m='be')
        res = pbs.render_optional_directives_block()

        self.assertIn('#PBS -m be', res)

    def test_load_module(self):
        pbs = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=10, max_time=20,
                        m='be')

        # load anaconda module
        pbs.load_module('anaconda')

        # get all the modules
        ret = pbs.get_modules()

        self.assertEqual('anaconda', ret['load'])

    def test_unload_module(self):
        pbs = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=10, max_time=20,
                        m='be')

        # unload C++ module
        pbs.unload_module('C++')

        # get all the modules
        ret = pbs.get_modules()

        self.assertEqual('C++', ret['unload'])

    def test_swap_module(self):
        pbs = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=10, max_time=20,
                        m='be')

        # load modules
        pbs.load_module('OpenMP')
        pbs.swap_module('OpenMP', 'C++')

        # get all the modules
        ret = pbs.get_modules()

        self.assertEqual('C++', ret['load'])

    def test_render_modules_block(self):
        pbs = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=10, max_time=20,
                        m='be')

        # load anaconda module
        pbs.load_module('anaconda')

        # unload C++ module
        pbs.unload_module('C++')

        # swap modules
        pbs.swap_module('anaconda', 'OpenMP')

        ret = pbs.render_modules_block()

        self.assertIn('module load OpenMP', ret)
        self.assertIn('module swap anaconda,OpenMP', ret)
        self.assertIn('module unload C++', ret)

    def test_render(self):
        pbs = PbsScript(job_name='test1', project_id='P001', num_nodes=5, processes_per_node=10, max_time=20,
                        m='be')

        render_str = pbs.render()

        print(render_str)



if __name__ == '__main__':
    unittest.main()

