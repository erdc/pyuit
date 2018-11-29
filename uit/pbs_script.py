import collections
import os
import io


PbsDirective = collections.namedtuple('PbsDirective', ['directive', 'options'])


class PbsScript(object):
    def __init__(self, job_name, project_id, num_nodes, processes_per_node, max_time,
                 queue='debug', node_type='compute', system='topaz'):
        self.name = job_name
        self.project_id = project_id
        self.num_nodes = num_nodes
        self.processes_per_node = processes_per_node
        self.max_time = max_time
        self.queue = queue
        self.node_type = node_type

        node_types = ['compute', 'gpu', 'bigmem', 'transfer', 'knl']

        if node_type.lower() not in node_types:
            raise ValueError('Please specify a valid node type: {}'.format(', '.join(node_types)))
        self.node_type = node_type.lower()

        systems = ['topaz', 'onyx']
        if system.lower() not in systems:
            raise ValueError('Please specify a valid system: {}'.format(', '.join(systems)))
        self.system = system.lower()

        if self.node_type.lower() == 'knl' and self.system != 'onyx':
            raise ValueError('KNL node types are only valid on Onyx.')

        self._optional_directives = []
        self._modules = {}
        self.execution_block = ""

    def set_directive(self, directive, value):
        """
        Append new directive to _optional_directives list

        Parameters
        ----------
        directive:  str
            name of the directive
        value:  str
            value of directive
        """
        self._optional_directives.append(PbsDirective(directive, value))

    def get_directive(self, directive):
        """
        Returns value of named directive from _optional_directives list
        Parameters
        ----------
        directive: str
            name of the directive
        Returns
        -------
        value of the given directory
        """
        directives = self._optional_directives
        for i in range(len(directives)):
            if directives[i].directive == directive:
                return directives[i].options

    def get_directives(self):
        """
        Returns _optional_directives list
        """
        return self._optional_directives

    def render_required_directives_block(self):
        """
        Returns string with each required directive rendered on a separate line

        Returns
        -------
        string of directive_block
        """
        pbs_dir_start = "## Required PBS Directives --------------------------------"
        job_name = "#PBS -N " + self.job_name
        project_id = "#PBS -A " + self.project_id
        queue = "#PBS -q " + self.queue

        no_nodes_process = self.get_no_nodes_process_str()
        time_to_run = "#PBS -l walltime={}".format(self.max_time)
        directive_block = pbs_dir_start + "\n" + \
                          job_name + "\n" + \
                          project_id + "\n" + \
                          queue + "\n" + \
                          no_nodes_process + "\n" + \
                          time_to_run
        return directive_block

    def get_no_nodes_process_str(self):
        """
        Returns the correct Number of Nodes and Processes per Node based on system, node type, number of nodes, and process per node
        Returns
        -------
        return a block of string for directives_block
        """
        if self.system == 'onyx':
            if self.node_type == 'compute':
                processes_per_node = [1, 2, 4, 11, 22, 44]
                if self.processes_per_node in processes_per_node:
                    ncpus = 44
                    no_nodes_process_str = '#PBS -l select={}:ncpus={}:mpiprocs={}'.format(
                        self.num_nodes, ncpus, self.processes_per_node
                    )
                    return no_nodes_process_str
                else:
                    raise ValueError('Please specify valid self.processes_per_node for the given system [Onyx]')
            if self.node_type == 'gpu':
                processes_per_node = [1, 2, 11, 22]
                if self.processes_per_node in processes_per_node:
                    ncpus = 22
                    no_nodes_process_str = '#PBS -l select={}:ncpus={}:mpiprocs={}2:ngpus=1'.format(
                        self.num_nodes, ncpus, self.processes_per_node
                    )
                    return no_nodes_process_str
                else:
                    raise ValueError('Please specify valid self.processes_per_node for the given node '
                                     'type [GPU] and System [Onyx]')
            if self.node_type == 'bigmem':
                processes_per_node = [1, 2, 4, 11, 22, 44]
                if self.processes_per_node in processes_per_node:
                    ncpus = 44
                    no_nodes_process_str = '#PBS -l select={}:ncpus={}:mpiprocs={}:bigmem=1'.format(
                        self.num_nodes, ncpus, self.processes_per_node
                    )
                    return no_nodes_process_str
                else:
                    raise ValueError('Please specify valid self.processes_per_node for the given node type [bigmem] '
                                     'and System [Onyx]')
            if self.node_type == "transfer":
                no_nodes_process_str = '#PBS -l select=1:ncpus=1'
                return no_nodes_process_str
            if self.node_type == 'knl':
                processes_per_node = [1, 2, 4, 8, 16, 32, 64]
                if self.processes_per_node in processes_per_node:
                    ncpus = 64
                    no_nodes_process_str = '#PBS -l select={}:ncpus={}:mpiprocs={}:nmics=1'.format(
                        self.num_nodes, ncpus, self.processes_per_node
                    )
                    return no_nodes_process_str
                else:
                    raise ValueError('Please specify valid self.processes_per_node for the given node type [knl] '
                                     'and System [Onyx]')
        elif self.system == 'topaz':
            if self.node_type == 'compute':
                ncpus = 36
                no_nodes_process_str = '#PBS -l select={}:ncpus={}:mpiprocs={}'.format(
                    self.num_nodes, ncpus, self.processes_per_node
                )
                return no_nodes_process_str
            if self.node_type == 'gpu':
                ncpus = 28
                no_nodes_process_str = '#PBS -l select={}:ncpus={}:mpiprocs={}:ngpus=1'.format(
                    self.num_nodes, ncpus, self.processes_per_node
                )
                return no_nodes_process_str
            if self.node_type == 'bigmem':
                ncpus = 32
                no_nodes_process_str = '#PBS -l select={}:ncpus={}:mpiprocs={}:bigmem=1'.format(
                    self.num_nodes, ncpus, self.processes_per_node
                )
                return no_nodes_process_str
            if self.node_type == "transfer":
                no_nodes_process_str = '#PBS -l select=1:ncpus=1'
                return no_nodes_process_str

    def render_optional_directives_block(self):
        """
        Returns string with each optional directive rendered on a separate line

        Returns
        -------
        string with each optional directive
        """
        opt_list = ['## Optional Directives -----------------------------']
        directives = self._optional_directives
        for i in range(len(directives)):
            opt_list.append("#PBS " + directives[i].directive + " " + directives[i].options)

        render_opt_dir_block = '\n'.join(map(str, opt_list))

        return render_opt_dir_block

    def load_module(self, module):
        """
        Adds module to _modules with value of “load”
        """
        self._modules.update({module: "load"})

    def unload_module(self, module):
        """
        Adds module to _modules with value of “unload”
        """
        self._modules.update({module: "unload"})

    def swap_module(self, module1, module2):
        """
        Adds module1 to _modules with value of module2.
        """
        self._modules.update({module1: module2})

    def get_modules(self):
        """
        Returns the _modules dictionary.
        """
        return self._modules

    def render_modules_block(self):
        """
         Returns string with each module call rendered on a separate line.
        """
        opt_list = ['## Modules --------------------------------------']
        for key, value in self._modules.items():
            if value != 'load' and value != 'unload':
                str_module = "module swap " + key + " " + value
            else:
                str_module = "module " + value + " " + key
            opt_list.append(str_module)

        str_render_modules_block = '\n'.join(map(str, opt_list))

        return str_render_modules_block

    def render(self):
        """
          Return string of fully rendered PBS Script.
        """
        shebang = "#!/bin/bash"
        render_required_directives = self.render_required_directives_block()
        render_optional_directives = self.render_optional_directives_block()
        render_modules_block = self.render_modules_block()
        render_execution_block = self.execution_block
        render_string = shebang + "\n \n" + \
                        render_required_directives + "\n \n" + \
                        render_optional_directives + "\n \n" + \
                        render_modules_block + "\n \n" + \
                        render_execution_block
        return render_string

    def write(self, path):
        """
        Calls render() method and write resulting string to the file path given.
        """
        render_string = self.render()
        # Open the file
        full_path = os.path.join(path)
        outfile = io.open(full_path, 'w', newline='\n')
        outfile.write(render_string)
