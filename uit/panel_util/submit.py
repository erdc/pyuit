import yaml
from collections import OrderedDict
from pathlib import Path

import param
import panel as pn
import pandas as pd

from .file_browser import HpcFileBrowser
from ..uit import Client, QUEUES
from ..pbs_script import NODE_TYPES, factors, PbsScript


class PbsScriptInputs(param.Parameterized):
    hpc_subproject = param.ObjectSelector(default=None, precedence=3)
    workdir = param.String(default='', precedence=4)
    node_type = param.ObjectSelector(default='', objects=[], precedence=5)
    nodes = param.Integer(default=1, bounds=(1, 100), precedence=5.1)
    processes_per_node = param.ObjectSelector(default=1, objects=[], precedence=5.2)
    wall_time = param.String(default='00:05:00', precedence=6)
    queue = param.ObjectSelector(default='debug', objects=QUEUES, precedence=7)
    submit_script_filename = param.String(default='run.pbs', precedence=8)

    def update_hpc_conneciton_dependent_defaults(self):
        if not self.uit_client.connected:
            return

        subprojects = [u['subproject'] for u in self.uit_client.show_usage()]
        self.param.hpc_subproject.objects = subprojects
        self.hpc_subproject = subprojects[0]
        self.workdir = self.uit_client.WORKDIR.as_posix()
        self.param.node_type.objects = list(NODE_TYPES[self.uit_client.system].keys())
        self.node_type = self.param.node_type.objects[0]
        self.param.queue.objects = self.uit_client.get_queues()

    @param.depends('node_type', watch=True)
    def update_processes_per_node(self):
        self.param.processes_per_node.objects = factors(NODE_TYPES[self.uit_client.system][self.node_type])
        self.processes_per_node = self.param.processes_per_node.objects[-1]

    def pbs_options_view(self):
        self.update_hpc_conneciton_dependent_defaults()
        hpc_submit = pn.panel(self, parameters=list(PbsScriptInputs.param), show_name=False, name='PBS Options')
        return hpc_submit


class PbsScriptAdvancedInputs(param.Parameterized):
    modules_to_load = param.ListSelector(default=[])
    modules_to_unload = param.ListSelector(default=[])
    load_modules = param.List()
    unload_modules = param.List()
    environment_variables = param.ClassSelector(OrderedDict, default=OrderedDict())
    env_names = param.List()
    env_values = param.List()
    browse = param.Action(lambda self: self.toggle_file_browser(), label='📂')
    browse_toggle = param.Boolean(default=False)
    configuration_file = param.String()

    def update_hpc_conneciton_dependent_defaults_advanced(self):
        if not self.uit_client.connected:
            return

        self.load_config_file()
        self.param.modules_to_unload.objects = sorted(self.uit_client.get_loaded_modules())
        self.param.modules_to_load.objects = self._get_modules_available_to_load()
        self.modules_to_load = self._validate_modules(self.param.modules_to_load.objects, self.modules_to_load)
        self.unload_modules = self._validate_modules(self.param.modules_to_unload.objects, self.modules_to_unload)

    def _get_modules_available_to_load(self):
        modules = set(self.uit_client.get_available_modules(flatten=True)) - set(self.param.modules_to_unload.objects)
        return sorted(modules)

    def _validate_modules(self, possible, candidates):
        df = pd.DataFrame([v.split('/', 1) for v in possible], columns=['Name', 'Version'])
        df['Default'] = df['Version'].apply(lambda v: True if v is None else v.endswith('(default)'))
        dfg = df.groupby('Name')

        modules = list()
        for m in candidates:
            if m in possible:
                modules.append(m)
                continue
            elif m in dfg.groups:
                group = dfg.get_group(m)
                row = group[group['Default']].iloc[0]
                module = f'{row.Name}/{row.Version}'
                modules.append(module)
            else:
                print(f'Module "{m}" is  invalid.')
        return sorted(modules)

    def load_config_file(self):
        config_file = Path(self.configuration_file)
        if config_file.is_file():
            with config_file.open() as f:
                config = yaml.safe_load(f).get(self.uit_client.system, {})
            modules = config.get('modules')
            if modules:
                self.modules_to_load = modules.get('load') or self.modules_to_load
                self.modules_to_unload = modules.get('unload') or self.modules_to_unload
            self.environment_variables = OrderedDict(config.get('environment_variables')) or self.environment_variables

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

    @param.depends('environment_variables', watch=True)
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
        self.update_hpc_conneciton_dependent_defaults_advanced()
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
            name='Advanced',
        )


class HpcSubmit(PbsScriptInputs, PbsScriptAdvancedInputs):
    submit_btn = param.Action(lambda self: self._submit(), label='Submit', precedence=10)
    job_name = param.String()
    uit_client = param.ClassSelector(Client)
    _pbs_script = param.ClassSelector(PbsScript, default=None)

    def submit(self):
        pass

    def _submit(self):
        if not self.param.submit_btn.constant:
            self.param.submit_btn.label = 'Submitted: Click "next" to Continue'
            self.param.submit_btn.constant = True
            return self.submit()

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
        #  TODO remove "(default)" from any modules
        for module in self.modules_to_load:
            self._pbs_script.load_module(module)
        for module in self.modules_to_unload:
            self._pbs_script.unload_module(module)
        self._pbs_script._environment_variables = self.environment_variables
        self._pbs_script.execution_block = self.execution_block()
        return self._pbs_script

    def execution_block(self):
        return ''

    def submit_view(self):
        return pn.Column(
            self.view,
            pn.Param(
                self,
                parameters=['submit_btn'],
                widgets={'submit_btn': {'button_type': 'success', 'width': 200}},
                show_name=False,
            ),
            name='Submit',
        )

    def view(self):
        return self.param.job_name,

    def panel(self):
        return pn.Column(
            '<h1>Submit Job</h1>',
            pn.layout.Tabs(
                self.submit_view(),
                self.pbs_options_view(),
                self.advanced_options_view(),
            ),
        )
