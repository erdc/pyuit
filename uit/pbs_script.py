import collections
import os
import io

NODE_TYPES = {
    'topaz': {
        'compute': 36,
        'gpu': 28,
        'bigmem': 32,
        'transfer': 1,
    },
    'onyx': {
        'compute': 44,
        'gpu': 22,
        'bigmem': 44,
        'transfer': 1,
        'knl': 64,
    }
}


def factors(n):
    return sorted(set([j for k in [[i, n // i] for i in range(1, int(n ** 0.5) + 1) if not n % i] for j in k]))


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
        array_indices (tuple): Indices for a job array in the following format: (start, end, [step]).
            e.g. (0, 9) or (0, 9, 3)
    """
    def __init__(self, name, project_id, num_nodes, processes_per_node, max_time,
                 queue='debug', node_type='compute', system='onyx', array_indices=None):

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
        self.node_type = node_type.lower()
        self.system = system.lower()
        self._array_indices = array_indices

        self._validate_system()
        self._validate_node_type()
        self._validate_processes_per_node()

        self._optional_directives = []
        self._modules = {}
        self.execution_block = ""

    def _validate_system(self):
        systems = list(NODE_TYPES.keys())
        if self.system not in systems:
            raise ValueError(f'Please specify a valid system. Must be one of: {systems}')

    def _validate_node_type(self):
        node_types = list(NODE_TYPES[self.system].keys())
        if self.node_type not in node_types:
            raise ValueError(f'Please specify a valid node type: {node_types}')

    def _validate_processes_per_node(self):
        processes_per_node = factors(NODE_TYPES[self.system][self.node_type])
        if self.processes_per_node not in processes_per_node:
            raise ValueError(f'Please specify valid "processes_per_node" for the given node type [{self.node_type}] '
                             f'and System [{self.system}].\nMust be one of: {processes_per_node}')


    @property
    def get_num_nodes_process_directive(self):
        """Generate a properly formatted CPU Request for use in PBS Headers.

        Generates string based on:
        - Number of Nodes
        - Processors per Node
        - System
        - Node Type

        Returns:
            str: Correctly formatted string for PBS header
        """
        processes_per_node = factors(NODE_TYPES[self.system][self.node_type])
        self._validate_processes_per_node()
        ncpus = max(processes_per_node)
        no_nodes_process_options = f'select={self.num_nodes}:ncpus={ncpus}'
        if self.node_type != 'transfer':
            no_nodes_process_options += f':mpiprocs={self.processes_per_node}'
        node_type_args = dict(
            gpu='ngpus',
            bigmem='bigmem',
            knl='nmics',
        )
        if self.node_type in node_type_args:
            no_nodes_process_options += f':{node_type_args[self.node_type]}=1'

        return PbsDirective('-l', no_nodes_process_options)

    @property
    def job_array_directives(self):
        if self._array_indices is not None:
            options = f'{self._array_indices[0]}-{self._array_indices[1]}'
            try:
                options += f':{self._array_indices[2]}'
            except IndexError:
                pass
            return PbsDirective('-J', options), PbsDirective('-r', 'y')

    @property
    def job_array_indices(self):
        if self._array_indices is not None:
            indices = list(self._array_indices)
            indices[1] += 1  # unlike Python PBS is inclusive of the last index
            return list(range(*indices))

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
        header = "## Required PBS Directives --------------------------------"
        directives = [
            PbsDirective('-N', self.name),
            PbsDirective('-A', self.project_id),
            PbsDirective('-q', self.queue),
            self.get_num_nodes_process_directive,
            PbsDirective('-l', f'walltime={self.max_time}'),
        ]
        if self._array_indices is not None:
            directives.extend(self.job_array_directives)

        return self._render_directive_list(header, directives)

    def render_optional_directives_block(self):
        """Render each optional directive on a separate line.

        Returns:
             str: All optional directives.
        """
        header = '## Optional Directives -----------------------------'
        return self._render_directive_list(header, self._optional_directives)

    @staticmethod
    def _render_directive_list(header, directives):
        lines = [header]
        for directive in directives:
            lines.append(f"#PBS {directive.directive} {directive.options}")

        return '\n'.join(lines)

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
