import collections
import os
import io


class PbsScript(object):
    def __init__(self, job_name, project_id, num_nodes, processes_per_node, max_time,
                 ncpus=36, queue='debug', node_type='compute', system='topaz', **kwargs):
        self.job_name = job_name
        self.project_id = project_id
        self.ncpus = ncpus
        self.num_nodes = num_nodes
        self.processes_per_node = processes_per_node
        self.max_time = max_time
        self.queue = queue
        self.node_type = node_type
        self.system = system
        self._optional_directives = []
        self._modules = {}
        self.execution_block = " "
        if kwargs:
            self.directive_options = collections.namedtuple('directive', ['directive', 'options'])
            for directive, options in kwargs.items():
                self._optional_directives.append(self.directive_options("-" + directive, options))

    def set_directive(self, directive, value):
        """
        Append new directive to _optional_directives list and save

        Parameters
        ----------
        directive:  str
            name of the directive
        value:  str
            value of directive
        """
        # append the directive to the _optional_directive
        self._optional_directives.append(self.directive_options(directive, value))

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
        no_nodes_process = "#PBS -l select={}:ncpus={}:mpiprocs={}".format(self.num_nodes, self.ncpus,
                                                                           self.processes_per_node)
        time_to_run = "#PBS -l walltime={}".format(self.max_time)
        directive_block = pbs_dir_start + "\n" + \
                          job_name + "\n" + \
                          project_id + "\n" + \
                          queue + "\n" + \
                          no_nodes_process + "\n" + \
                          time_to_run
        return directive_block

    def render_optional_directives_block(self):
        """
        Returns string with each optional directive rendered on a separate line

        Returns
        -------
        string with each optional directive
        """
        opt_list = []
        opt_list.append('## Optional Directives -----------------------------')
        directives = self._optional_directives
        for i in range(len(directives)):
            opt_list.append("#PBS " + directives[i].directive + " " + directives[i].options)

        render_opt_dir_block = '\n'.join(str(e) for e in opt_list)

        return render_opt_dir_block

    def load_module(self, module):
        """
        Adds module to _modules with value of “load”
        """
        self._modules.update({"load": module})

    def unload_module(self, module):
        """
        Adds module to _modules with value of “unload”
        """
        self._modules.update({"unload": module})

    def swap_module(self, module1, module2):
        """
        Adds module1 to _modules with value of module2.
        """
        for k, v in self._modules.items():
            if v == module1:
                self._modules[k] = module2

        swap_modules = module1 + "," + module2
        # TODO: check with Nathan: adding swap modules to _modules_update
        self._modules.update({"swap": swap_modules})

    def get_modules(self):
        """
        Returns the _modules dictionary.
        """
        return self._modules

    def render_modules_block(self):
        """
         Returns string with each module call rendered on a separate line.
        """
        opt_list = []
        opt_list.append('## Modules --------------------------------------')
        for key, value in self._modules.items():
            opt_list.append("module " + key + " " + value)

        render_modules_block = '\n'.join(str(e) for e in opt_list)

        return render_modules_block

    # TODO: Check with Nathan. Do we need this?
    def exe_block(self):
        if self.execution_block is not None:
            return self.execution_block
        return None

    def render(self):
        """
        Return string of fully rendered PBS Script.
        """
        shebang = "#!/bin/bash"
        render_required_directives = self.render_required_directives_block()
        render_optional_directives = self.render_optional_directives_block()
        render_modules_block = self.render_modules_block()
        # TODO:  check with Nathan: about render execution block
        render_execution_block = self.exe_block()
        render_string = shebang + "\n" + \
                        render_required_directives + "\n" + \
                        render_optional_directives + "\n" + \
                        render_modules_block + "\n" + \
                        render_execution_block + "\n"
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
