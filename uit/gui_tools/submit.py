import param
import panel as pn

from .file_browser import HpcFileBrowser
from .configurable import HpcConfigurable
from ..uit import Client, QUEUES
from ..pbs_script import NODE_TYPES, factors, PbsScript


class PbsScriptInputs(param.Parameterized):
    hpc_subproject = param.ObjectSelector(default=None, precedence=3)
    workdir = param.String(default='', precedence=4)
    node_type = param.ObjectSelector(default='', objects=[], precedence=5)
    nodes = param.Integer(default=1, bounds=(1, 100), precedence=5.1)
    processes_per_node = param.ObjectSelector(default=1, objects=[], precedence=5.2)
    wall_time = param.String(default='00:05:00', precedence=6)
    queue = param.ObjectSelector(default=QUEUES[0], objects=QUEUES, precedence=7)
    submit_script_filename = param.String(default='run.pbs', precedence=8)

    def update_hpc_connection_dependent_defaults(self):
        if not self.uit_client.connected:
            return

        subprojects = [u['subproject'] for u in self.uit_client.show_usage()]
        self.param.hpc_subproject.objects = subprojects
        self.hpc_subproject = subprojects[0]
        self.workdir = self.uit_client.WORKDIR.as_posix()
        self.param.node_type.objects = list(NODE_TYPES[self.uit_client.system].keys())
        self.node_type = self.param.node_type.objects[0]
        self.param.queue.objects = self.uit_client.get_queues()
        self.queue = self.queue if self.queue in self.param.queue.objects else self.param.queue.objects[0]

    @param.depends('node_type', watch=True)
    def update_processes_per_node(self):
        self.param.processes_per_node.objects = factors(NODE_TYPES[self.uit_client.system][self.node_type])
        self.processes_per_node = self.param.processes_per_node.objects[-1]

    def pbs_options_view(self):
        self.update_hpc_connection_dependent_defaults()
        hpc_submit = pn.panel(
            self,
            parameters=list(PbsScriptInputs.param)[1:],  # all params except 'name'
            widgets={'nodes': pn.widgets.Spinner},
            show_name=False,
            name='PBS Options'
        )
        return hpc_submit


class PbsScriptAdvancedInputs(HpcConfigurable):
    env_names = param.List()
    env_values = param.List()
    browse = param.Action(lambda self: self.toggle_file_browser(), label='📂')
    browse_toggle = param.Boolean(default=False)

    def update_environ(self, *events):
        for event in events:
            _, is_key, i = event.obj.css_classes[0].split('_')
            is_key = is_key == 'key'
            i = int(i)
            if is_key:
                if i > -1:
                    self.environment_variables[event.new] = self.environment_variables[event.old]
                    del self.environment_variables[event.old]
                else:
                    self.environment_variables[event.new] = None
            else:
                key = self.env_names[i].value
                self.environment_variables[key] = event.new

        self.param.trigger('environment_variables')

    def env_var_widget(self, val, tag, **kwargs):
        widget = pn.widgets.TextInput(value=val, css_classes=[tag], **kwargs)
        widget.param.watch(self.update_environ, ['value'], onlychanged=True)
        return widget

    def toggle_file_browser(self):
        self.browse_toggle = not self.browse_toggle

    @param.depends('browse_toggle')
    def file_browser_view(self):
        if self.browse_toggle:
            return HpcFileBrowser(self.uit_client).panel
        else:
            return pn.layout.Spacer()

    @param.depends('environment_variables')
    def environment_variables_view(self):
        self.env_names = list()
        self.env_values = list()
        for i, (k, v) in enumerate(self.environment_variables.items()):
            name_widget = self.env_var_widget(val=k, tag=f'env_key_{i}')
            val_widget = self.env_var_widget(val=str(v), tag=f'env_val_{i}')
            self.env_names.append(name_widget)
            self.env_values.append(val_widget)

        self.env_names.append(self.env_var_widget(val=None, tag='env_key_-1', placeholder='NEW_ENV_VAR'))
        self.env_values.append(self.env_var_widget(val=None, tag='env_val_-1', disabled=True))

        self.env_names[0].name = 'Name'
        self.env_values[0].name = 'Value'

        return pn.Column(
            '<h3>Environment Variables</h3>',
            pn.Column(
                *[pn.Row(k, v, width_policy='max') for k, v in zip(self.env_names, self.env_values)],
            ),
            pn.Row(
                '<h4>File Browser</h4>',
                pn.Param(self, parameters=['browse'], widgets={'browse': {'width': 30}}, show_name=False),
            ),
            self.file_browser_view,
        )

    def advanced_options_view(self):
        return pn.Row(
            pn.Column(
                '<h3>Modules to Load</h3>',
                pn.Param(
                    self,
                    parameters=['modules_to_load'],
                    widgets={'modules_to_load': pn.widgets.CrossSelector},
                    width=700,
                    show_name=False
                ),
                '<h3>Modules to Unload</h3>',
                pn.Param(
                    self,
                    parameters=['modules_to_unload'],
                    widgets={'modules_to_unload': pn.widgets.CrossSelector},
                    width=700,
                    show_name=False
                ),
            ),
            self.environment_variables_view,
            name='Environment',
        )


class HpcSubmit(PbsScriptInputs, PbsScriptAdvancedInputs):
    submit_btn = param.Action(lambda self: self._submit(), label='Submit', constant=True, precedence=10)
    validate_btn = param.Action(lambda self: self._validate(), label='Validate', constant=True, precedence=10)
    disable_validation = param.Boolean()
    validated = param.Boolean()
    job_name = param.String(label='Job Name (Required)')
    uit_client = param.ClassSelector(Client)
    _pbs_script = param.ClassSelector(PbsScript, default=None)
    ready = param.Boolean(default=False, precedence=-1)

    def pre_validate(self):
        pass

    def pre_submit(self):
        pass

    def submit(self):
        return None

    def _submit(self):
        if not self.param.submit_btn.constant:
            self.param.submit_btn.constant = True
            self.pre_submit()
            result = self.submit()
            self.ready = bool(result)
            return result

    def validate(self):
        return True

    def _validate(self):
        if not self.param.validate_btn.constant:
            self.param.validate_btn.constant = True
            self.pre_validate()
            result = self.validate()
            self.validated = result

    @property
    def pbs_script(self):
        self._pbs_script = PbsScript(
            name=self.job_name,
            project_id=self.hpc_subproject,
            num_nodes=self.nodes,
            queue=self.queue,
            processes_per_node=self.processes_per_node,
            node_type=self.node_type,
            max_time=self.wall_time,
            system=self.uit_client.system,
        )

        # remove "(default)" from any modules when adding to pbs script
        for module in self.modules_to_load:
            self._pbs_script.load_module(module.replace('(default)', ''))
        for module in self.modules_to_unload:
            self._pbs_script.unload_module(module.replace('(default)', ''))
        self._pbs_script._environment_variables = self.environment_variables
        self._pbs_script.execution_block = self.execution_block
        return self._pbs_script

    @property
    def execution_block(self):
        return ''

    @param.depends('job_name', watch=True)
    def is_submitable(self):
        self.param.submit_btn.constant = self.param.validate_btn.constant = not bool(self.job_name)

    @param.depends('disable_validation', 'validated')
    def action_button(self):
        if self.disable_validation or self.validated:
            button = 'submit_btn'
            button_type = 'success'
        else:
            button = 'validate_btn'
            button_type = 'primary'

        return pn.Param(
            self.param[button],
            widgets={button: {'button_type': button_type, 'width': 200}}
        )

    def submit_view(self):
        return pn.Column(
            self.view,
            self.action_button,
            name='Submit',
        )

    def view(self):
        return pn.panel(self.param.job_name),

    def panel(self):
        return pn.Column(
            '# Submit Job',
            pn.layout.Tabs(
                self.submit_view(),
                self.pbs_options_view(),
                self.advanced_options_view(),
            ),
        )
