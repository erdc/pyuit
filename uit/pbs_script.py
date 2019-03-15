import collections
import os
import io


PbsDirective = collections.namedtuple('PbsDirective', ['directive', 'options'])


class PbsScript(object):
    """
    Generates a PBS script needed to submit jobs.

    Attributes:
        max_time (str): Maximum amount of time the job should be allowed to run.
        name (str): Name of the job to be passed in the PBS Header.
        node_type (str): Type of node on which the job should run.
        num_nodes (int): Number of nodes to request.
        processes_per_node (int): Number of processors per node to request.
        project_id (str): Project ID to be passed in the PBS Header.
        queue (str): Name of the queue into which to submit the job.
        system (str): Name of the system to run on.
    """
    def __init__(self, name, project_id, num_nodes, processes_per_node, max_time,
                 queue='debug', node_type='compute', system='topaz'):

        if not name:
            raise ValueError('Parameter "name" is required.')

        if not project_id:
            raise ValueError('Parameter "project_id" is required.')

        if not max_time:
            raise ValueError('Parameter "max_time" is required.')

        self.name = name
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
        """Add a new directive to the PBS script.

        Args:
            directive (str): Name of the directive.
            value (str): Value of directive.
        """
        self._optional_directives.append(PbsDirective(directive, value))

    def get_directive(self, directive):
        """Get value of named directive.

        Args:
            directive (str): Name of the directive.

        Returns:
            str: Value of the given directive.
        """
        for d in self._optional_directives:
            if d.directive == directive:
                return d.options

    def get_directives(self):
        """Get a list of all defined directives.

        Returns:
             list: All defined directives.
        """
        return self._optional_directives

    def load_module(self, module):
        """Add a load directive to the PBS script for the given module.

        Args:
            module (str): Name of the module to load
        """
        self._modules.update({module: "load"})

    def unload_module(self, module):
        """Add an unload directive to the PBS script for the given module.

        Args:
            module (str): Name of the module to unload
        """
        self._modules.update({module: "unload"})

    def swap_module(self, module1, module2):
        """Add a swap directive to the PBS script for the given modules.

        Args:
            module1 (str): Name of the module to be swapped out
            module2 (str): Name of the module to be swapped in
        """
        self._modules.update({module1: module2})

    def get_modules(self):
        """Get a list of all modules.

        Returns:
             dict<module,command>: A dictionary of modules with module as the key and the command (load/unload) as the value. In the case of a swap, the value will be the module to replace the module listed as the key.
        """  # noqa: E501
        return self._modules

    def render_required_directives_block(self):
        """Render each required directive on a separate line.

        Returns:
            str: String of all required directives.
        """
        pbs_dir_start = "## Required PBS Directives --------------------------------"
        job_name = "#PBS -N " + self.name
        project_id = "#PBS -A " + self.project_id
        queue = "#PBS -q " + self.queue

        no_nodes_process = self.get_num_nodes_process_str()
        time_to_run = "#PBS -l walltime={}".format(self.max_time)
        directive_block = pbs_dir_start + "\n" + job_name + "\n" + project_id + "\n" + \
            queue + "\n" + no_nodes_process + "\n" + time_to_run
        return directive_block

    def get_num_nodes_process_str(self):
        """Generate a properly formatted CPU Request for use in PBS Headers.

        Generates string based on:
        - Number of Nodes
        - Processors per Node
        - System
        - Node Type

        Returns:
            str: Correctly formatted string for PBS header
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
        """Render each optional directive on a separate line.

        Returns:
             str: All optional directives.
        """
        opt_list = ['## Optional Directives -----------------------------']
        directives = self._optional_directives
        for i in range(len(directives)):
            opt_list.append("#PBS " + directives[i].directive + " " + directives[i].options)

        render_opt_dir_block = '\n'.join(map(str, opt_list))

        return render_opt_dir_block

    def render_modules_block(self):
        """Render each module call on a separate line.

        Returns:
            str: All module calls.
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
        """Render the PBS Script.

        Returns:
            str: A fully rendered PBS Script.
        """
        shebang = "#!/bin/bash"
        render_required_directives = self.render_required_directives_block()
        render_optional_directives = self.render_optional_directives_block()
        render_modules_block = self.render_modules_block()
        render_execution_block = self.execution_block
        render_string = shebang + "\n \n" + render_required_directives + "\n \n" + render_optional_directives + \
            "\n \n" + render_modules_block + "\n \n" + render_execution_block
        return render_string

    def write(self, path):
        """Render the PBS Script and write to given file.

        Args:
            path (str): File to write out to.
        """
        render_string = self.render()
        # Open the file
        full_path = os.path.join(path)
        with io.open(full_path, 'w', newline='\n') as outfile:
            outfile.write(render_string)
