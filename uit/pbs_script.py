import collections
import datetime
import os
import io


NODE_TYPES = {
    'jim': {
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


NODE_ARGS = dict(
    compute='compute',
    gpu='ngpus',
    bigmem='bigmem',
    knl='nmics',
)


def factors(n):
    return sorted(set([j for k in [[i, n // i] for i in range(1, int(n ** 0.5) + 1) if not n % i] for j in k]))


PbsDirective = collections.namedtuple('PbsDirective', ['directive', 'options'])


class PbsScript(object):
    """
    Generates a PBS script needed to submit jobs.

    Attributes:
        name (str|required): Name of the job to be passed in the PBS Header.
        project_id (str|required): Project ID to be passed in the PBS Header.
        num_nodes (int|required): Number of nodes to request.
        processes_per_node (int|required): Number of processors per node to request.
        max_time (datetime.timedelta or str|required): Maximum amount of time the job should be allowed to run. If passed as a string it should be in the form "HH:MM:SS".
        node_type (str): Type of node on which the job should run (default='debug').
        queue (str): Name of the queue into which to submit the job (default='compute').
        system (str): Name of the system to run on (default='onyx').
        array_indices (tuple): Indices for a job array in the following format: (start, end, [step])
            e.g. (0, 9) or (0, 9, 3) (default=None).
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
        self._max_time = max_time
        self.queue = queue
        self.node_type = node_type.lower()
        self.system = system.lower()
        self._array_indices = array_indices

        self._validate_system()
        self._validate_node_type()
        self._validate_processes_per_node()

        self._optional_directives = []
        self._modules = {}
        self._module_use = []
        self._environment_variables = collections.OrderedDict()
        self._execution_block = None
        self.execution_block = ''  # User defined execution block
        self.configure_job_dir = False

    def __repr__(self):
        return f'<{self.__class__.__name__} name={self.name}>'

    def __str__(self):
        return self.render()

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

    @staticmethod
    def _create_block_header_string(header):
        header += ' '
        return f'## {header.ljust(50, "-")}'

    @property
    def max_time(self):
        return self._max_time

    @max_time.setter
    def max_time(self, max_time):
        if not isinstance(max_time, datetime.timedelta):
            try:
                parts = [int(p) for p in max_time.split(':')]
                hours, minutes, seconds = [0, 0, *parts][-3:]
                self._max_time = datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds)
            except:
                raise ValueError('max_time must be a datetime.timedelta or a string in the form "HH:MM:SS"')

    @property
    def walltime(self):
        hours = self.max_time.days * 24 + self.max_time.seconds // 3600
        minutes = self.max_time.seconds % 3600 // 60
        seconds = self.max_time.seconds % 3600 % 60
        return f'{hours}:{minutes:02}:{seconds:02}'

    @property
    def environment_variables(self):
        return self._environment_variables

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
        self._validate_processes_per_node()
        ncpus = NODE_TYPES[self.system][self.node_type]
        no_nodes_process_options = f'select={self.num_nodes}:ncpus={ncpus}'
        if self.node_type != 'transfer':
            no_nodes_process_options += f':mpiprocs={self.processes_per_node}'
        if self.node_type != 'compute' and self.node_type in NODE_ARGS:
            no_nodes_process_options += f':{NODE_ARGS[self.node_type]}=1'

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
    def number_of_sub_jobs(self):
        if self._array_indices is not None:
            return self._array_indices[1] + 1

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

    def get_directive(self, directive, first=False, default=None):
        """Get value of named directive.

        Args:
            directive (str): Name of the directive.
            first (bool|default=False): Return the options from the first instance of `directive` or `default`
                if no instances of `directive` are found.
            default (any|default=None): The default value to return if no instances of `directive` are found.

        Returns:
            List of options from all directives with the given directive. If `first=True` then returns single string.
        """
        options = list()
        for d in self.optional_directives:
            if d.directive == directive:
                options.append(d.options)
        if first:
            return [*options, default][0]

        return options

    @property
    def optional_directives(self):
        """Get a list of all defined directives.

        Returns:
             list: All defined directives.
        """
        return self._optional_directives

    def module_use(self, path):
        self._module_use.append(path)

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

    def set_environment_variable(self, key, value):
        self.environment_variables[key] = value

    def render_required_directives_block(self):
        """Render each required directive on a separate line.

        Returns:
            str: String of all required directives.
        """
        header = self._create_block_header_string('Required PBS Directives')
        directives = [
            PbsDirective('-N', self.name),
            PbsDirective('-A', self.project_id),
            PbsDirective('-q', self.queue),
            self.get_num_nodes_process_directive,
            PbsDirective('-l', f'walltime={self.walltime}'),
        ]
        if self._array_indices is not None:
            directives.extend(self.job_array_directives)

        return self._render_directive_list(header, directives)

    def render_optional_directives_block(self):
        """Render each optional directive on a separate line.

        Returns:
             str: All optional directives.
        """
        header = self._create_block_header_string('Optional Directives')
        return self._render_directive_list(header, self.optional_directives)

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
        opt_list = [self._create_block_header_string('Modules')]
        for path in self._module_use:
            opt_list.append(f'module use --append {path}')
        for key, value in self._modules.items():
            if value != 'load' and value != 'unload':
                str_module = "module swap " + key + " " + value
            else:
                str_module = "module " + value + " " + key
            opt_list.append(str_module)

        return '\n'.join(map(str, opt_list))

    def render_environment_block(self):
        opt_list = [self._create_block_header_string('Environment')]
        opt_list.extend([f'export {key}="{value}"' for key, value in self.environment_variables.items()])
        return '\n'.join(opt_list)

    def render_job_dir_configuration(self):
        if self.configure_job_dir:
            return '''
            JOBID=`echo ${PBS_JOBID} | cut -d '.' -f 1 | cut -d '[' -f 1`
            JOBDIR=$PBS_O_WORKDIR/$PBS_JOBNAME.$JOBID
            if [ ! -d ${JOBDIR} ]; then
              mkdir -p ${JOBDIR}
            fi
            # cd $JOBDIR
            
            '''
        return ''

    def render_execution_block(self):
        header = self._create_block_header_string('Execution Block')
        job_dir_config = self.render_job_dir_configuration()
        execution_block = self._execution_block or self.execution_block
        return header + '\n' + job_dir_config + execution_block

    def render(self):
        """Render the PBS Script.

        Returns:
            str: A fully rendered PBS Script.
        """
        shebang = "#!/bin/bash"
        render_required_directives = self.render_required_directives_block()
        render_optional_directives = self.render_optional_directives_block()
        render_modules_block = self.render_modules_block()
        render_environment_block = self.render_environment_block()
        render_execution_block = self.render_execution_block()
        render_string = '\n\n'.join([shebang, render_required_directives, render_optional_directives,
                                     render_modules_block, render_environment_block, render_execution_block])
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
