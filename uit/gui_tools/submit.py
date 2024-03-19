import re
import logging
from functools import partial
from itertools import zip_longest

import param
import panel as pn

from .file_browser import HpcFileBrowser
from .utils import HpcConfigurable
from ..uit import Client, QUEUES
from ..pbs_script import NODE_TYPES, factors, PbsScript
from ..job import PbsJob

logger = logging.getLogger(__name__)


class PbsScriptInputs(param.Parameterized):
    hpc_subproject = param.ObjectSelector(default=None, label='HPC Subproject', precedence=3)
    subproject_usage = param.DataFrame(precedence=3.1)
    workdir = param.String(default='', precedence=4)
    node_type = param.ObjectSelector(default='', objects=[], label='Node Type', precedence=5)
    nodes = param.Integer(default=1, bounds=(1, 1000), precedence=5.1)
    processes_per_node = param.ObjectSelector(default=1, objects=[], label='Processes per Node', precedence=5.2)
    wall_time = param.String(default='01:00:00', label='Wall Time', precedence=6)
    queue = param.ObjectSelector(default=QUEUES[0], objects=QUEUES, precedence=7)
    submit_script_filename = param.String(default='run.pbs', precedence=8)
    notification_email = param.String(label='Notification E-mail(s)', precedence=9)
    notify_start = param.Boolean(default=True, label='when job begins', precedence=9.1)
    notify_end = param.Boolean(default=True, label='when job ends', precedence=9.2)

    SHOW_USAGE_TABLE_MAX_WIDTH = 1030

    @staticmethod
    def get_default(value, objects):
        return value if value in objects else objects[0]

    def update_hpc_connection_dependent_defaults(self):
        if not self.uit_client.connected:
            return

        self.subproject_usage = self.uit_client.show_usage(as_df=True)
        subprojects = self.subproject_usage.subproject.to_list()
        self.param.hpc_subproject.objects = subprojects
        self.hpc_subproject = self.get_default(self.hpc_subproject, subprojects)
        self.workdir = self.uit_client.WORKDIR.as_posix()
        self.param.node_type.objects = list(NODE_TYPES[self.uit_client.system].keys())
        self.node_type = self.get_default(self.node_type, self.param.node_type.objects)
        self.param.queue.objects = self.uit_client.get_queues()
        self.queue = self.get_default(self.queue, self.param.queue.objects)

    @param.depends('queue', watch=True)
    def update_queue_depended_bounds(self):
        if self.queue == 'debug':
            self.wall_time = '00:10:00'

    @param.depends('node_type', watch=True)
    def update_processes_per_node(self):
        self.param.processes_per_node.objects = factors(NODE_TYPES[self.uit_client.system][self.node_type])
        self.processes_per_node = self.param.processes_per_node.objects[-1]

    def add_email_directives(self, pbs_script):
        if self.notify_start or self.notify_end:
            options = ''
            if self.notify_start:
                options += 'b'
            if self.notify_end:
                options += 'e'
            pbs_script.set_directive('-m', options)
        if self.notification_email:
            pbs_script.set_directive('-M', self.notification_email)

    def pbs_options_view(self):
        self.update_hpc_connection_dependent_defaults()
        return pn.Column(
            pn.Card(
                pn.widgets.Tabulator.from_param(self.param.subproject_usage, width=self.SHOW_USAGE_TABLE_MAX_WIDTH),
                title='Subproject Usage Summary',
                sizing_mode='stretch_width',
                collapsed=True,
            ),
            pn.Column(
                self.param.hpc_subproject,
                self.param.workdir,
                self.param.node_type,
                pn.widgets.Spinner.from_param(self.param.nodes),
                self.param.processes_per_node,
                self.param.wall_time,
                self.param.queue,
            ),
            pn.layout.WidgetBox(
                pn.widgets.TextInput.from_param(self.param.notification_email, placeholder='john.doe@example.com'),
                pn.pane.HTML('<label class"bk">Send e-mail notifications:</label>'),
                pn.widgets.Checkbox.from_param(self.param.notify_start, width=150),
                pn.widgets.Checkbox.from_param(self.param.notify_end, width=150),
            ),
            name='PBS Options'
        )


class PbsScriptAdvancedInputs(HpcConfigurable):
    env_names = param.List()
    env_values = param.List()
    env_browsers = param.List()
    env_delete_buttons = param.List()
    file_browser = param.ClassSelector(HpcFileBrowser)
    file_browser_col = param.ClassSelector(pn.Column, default=pn.Column(None, sizing_mode='stretch_width'))
    apply_file_browser = param.Action(label='Apply')
    close_file_browser = param.Action(lambda self: self.show_file_browser(False), label='Close')
    append_path = param.Boolean(label='Append to Path')

    @param.depends('uit_client', watch=True)
    def configure_file_browser(self):
        self.file_browser = HpcFileBrowser(self.uit_client)

    def show_file_browser(self, show):
        self.file_browser_col[0] = pn.WidgetBox(
            self.file_browser.panel,
            pn.Row(
                pn.widgets.Checkbox.from_param(self.param.append_path, width=100),
                pn.widgets.Button.from_param(
                    self.param.apply_file_browser,
                    button_type='success', width=100,
                ),
                pn.widgets.Button.from_param(
                    self.param.close_file_browser,
                    button_type='primary', width=100,
                ),
                align='end',
            ),
            sizing_mode='stretch_width',
        ) if show else None

        if not show:
            for btn in self.env_browsers:
                btn.loading = False

    def update_environ(self, event):
        _, widget_type, i = event.obj.css_classes[0].split('_')
        i = int(i)
        key = self.env_names[i].value
        if widget_type == 'key':
            if i > -1:
                self.environment_variables[event.new] = self.environment_variables[event.old]
                del self.environment_variables[event.old]
            else:
                self.environment_variables[event.new] = None
        elif widget_type == 'val':
            self.environment_variables[key] = event.new
        elif widget_type == 'del':
            del self.environment_variables[key]

        self.param.trigger('environment_variables')

    def env_var_widget(self, val, tag, **kwargs):
        widget = pn.widgets.TextInput(value=val, css_classes=[tag], **kwargs)
        widget.param.watch(self.update_environ, ['value'], onlychanged=True)
        return widget

    def env_file_browser_widget(self, tag, **kwargs):
        widget = pn.widgets.Button(name='📂', css_classes=[tag], width=40, align='end', **kwargs)
        widget.on_click(self.toggle_file_browser)
        return widget

    def env_delete_btn(self, tag):
        btn = pn.widgets.Button(name='X', css_classes=[tag], width=35, align='end', button_type='danger')
        btn.on_click(self.update_environ)
        btn.js_on_click(
            args={'btn': btn},
            code='btn.css_classes.push("pn-loading", "arc"); btn.properties.css_classes.change.emit();'
        )
        return btn

    def toggle_file_browser(self, event):
        button = event.obj
        button.loading = True
        _, is_key, i = button.css_classes[0].split('_')
        self.apply_file_browser = partial(self.update_file_path, index=int(i))
        self.show_file_browser(True)

    def update_file_path(self, _, index):
        if self.append_path:
            self.env_values[index].value += f':{self.file_browser.value[0]}'
        else:
            self.env_values[index].value = self.file_browser.value[0]

    @param.depends('environment_variables')
    def environment_variables_view(self):
        self.environment_variables.pop('', None)  # Clear blank key if there is one
        self.env_names = list()
        self.env_values = list()
        self.env_browsers = list()
        self.env_delete_buttons = list()

        for i, (k, v) in enumerate(self.environment_variables.items()):
            name_widget = self.env_var_widget(val=k, tag=f'env_key_{i}')
            val_widget = self.env_var_widget(val=str(v), tag=f'env_val_{i}')
            browser_widget = self.env_file_browser_widget(tag=f'env_browser_{i}')
            delete_btn = self.env_delete_btn(tag=f'env_del_{i}')
            self.env_names.append(name_widget)
            self.env_values.append(val_widget)
            self.env_browsers.append(browser_widget)
            self.env_delete_buttons.append(delete_btn)

        new_key_wg = self.env_var_widget(val=None, tag='env_key_-1', placeholder='NEW_ENV_VAR')
        new_val_wg = self.env_var_widget(val=None, tag='env_val_-1', disabled=True)
        new_key_wg.jscallback(
            args={'val': new_val_wg},
            value='val.css_classes.push("pn-loading", "arc"); val.properties.css_classes.change.emit();'
        )
        self.env_names.append(new_key_wg)
        self.env_values.append(new_val_wg)

        self.env_names[0].name = 'Name'
        self.env_values[0].name = 'Value'

        return pn.Card(
            *[pn.Row(k, v, b, d, sizing_mode='stretch_width') for k, v, b, d in
              zip_longest(self.env_names, self.env_values, self.env_browsers, self.env_delete_buttons)],
            self.file_browser_col,
            title='Environment Variables',
            sizing_mode='stretch_width',
        )

    def advanced_options_view(self):
        return pn.Column(
            self.environment_variables_view,
            pn.Card(
                '<h3>Modules to Load</h3>',
                pn.widgets.CrossSelector.from_param(self.param.modules_to_load, width=700),
                '<h3>Modules to Unload</h3>',
                pn.widgets.CrossSelector.from_param(self.param.modules_to_unload, width=700),
                title='Modules',
                sizing_mode='stretch_width',
                collapsed=True,
            ),
            name='Environment',
        )


class HpcSubmit(PbsScriptInputs, PbsScriptAdvancedInputs):
    submit_btn = param.Action(lambda self: self._submit(), label='Submit', constant=True, precedence=10)
    validate_btn = param.Action(lambda self: self._validate(), label='Validate', constant=True, precedence=10)
    cancel_btn = param.Action(lambda self: self.param.trigger('cancel_btn'), label='Cancel', precedence=10)
    previous_btn = param.Action(lambda self: self._previous(), label='Previous', precedence=10)
    disable_validation = param.Boolean(label='Override Validation')
    validated = param.Boolean()
    job_name = param.String(label='Job Name (Required, cannot contain spaces or tabs)')
    error_messages = param.ClassSelector(pn.Column, default=pn.Column(sizing_mode='stretch_width'))
    uit_client = param.ClassSelector(Client)
    _job = param.ClassSelector(PbsJob, default=None)
    ready = param.Boolean(default=False, precedence=-1)
    next_stage = param.Selector()
    pipeline_obj = param.ClassSelector(pn.pipeline.Pipeline)

    # TODO should this be here?  (see line 324)
    user_workspace = None

    def _previous(self):
        prev_stage = self.pipeline_obj._stages[self.pipeline_obj._prev_stage][0]
        prev_stage.reset()
        self.pipeline_obj.param.trigger('previous')
        self.pipeline_obj._block = False

    def pre_validate(self):
        pass

    def pre_submit(self):
        pass

    @param.output(jobs=list)
    def submit(self):
        if self.job:
            if not self.job.job_id:
                self.job.script = self.pbs_script  # update script to ensure it reflects any UI updates
                self.job.submit()
            return [self.job]

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
            is_valid = self.validate()
            self.validated = is_valid
            if is_valid:
                param.depends(
                    self.param.job_name,
                    self.param.environment_variables,
                    self.param.modules_to_load,
                    self.param.modules_to_unload,
                    watch=True
                )(self.un_validate)
            else:
                self.param.validate_btn.constant = False
                self.param.trigger('validated')

    def un_validate(self, *events):
        if self.validated:
            self.cancel()
            self.validated = False
            self.is_submitable()

    @param.depends('cancel_btn', watch=True)
    def triggered_cancel(self):
        self.cancel()

    def cancel(self):
        pass

    @property
    def pbs_script(self):
        pbs_script = PbsScript(
            name=self.job_name,
            project_id=self.hpc_subproject,
            num_nodes=self.nodes,
            queue=self.queue,
            processes_per_node=self.processes_per_node,
            node_type=self.node_type,
            max_time=self.wall_time,
            system=self.uit_client.system,
        )

        self.add_email_directives(pbs_script=pbs_script)

        # remove "(default)" from any modules when adding to pbs script
        for module in self.modules_to_load:
            pbs_script.load_module(module.replace('(default)', ''))
        for module in self.modules_to_unload:
            pbs_script.unload_module(module.replace('(default)', ''))

        pbs_script._environment_variables = self.environment_variables
        pbs_script.execution_block = self.execution_block

        return pbs_script

    @property
    def job(self):
        if self._job is None:
            self._job = PbsJob(script=self.pbs_script, client=self.uit_client, workspace=self.user_workspace)
        return self._job

    @property
    def execution_block(self):
        return ''

    @param.depends('job_name', 'environment_variables', watch=True)
    def is_submitable(self):
        self.error_messages[:] = []
        if not self.job_name:
            self.error_messages.append(
                pn.pane.Alert('* You must first enter a Job Name above before you can proceed.',
                              alert_type='danger')
            )
        elif re.match('^[^*&%\\/\s]*$', self.job_name) is None:  # noqa: W605
            self.error_messages.append(
                pn.pane.Alert('* Job Name cannot contain spaces or any of the following characters: * & % \\ /',
                              alert_type='danger')
            )
        errors_exist = len(self.error_messages) > 0
        self.param.submit_btn.constant = self.param.validate_btn.constant = \
            self.param.disable_validation.constant = errors_exist
        self.param.trigger('disable_validation')  # get buttons to reload

    @param.depends('disable_validation', 'validated')
    def action_button(self):
        if self.disable_validation or self.validated:
            button = 'submit_btn'
            button_type = 'success'
        else:
            button = 'validate_btn'
            button_type = 'primary'

        action_btn = pn.widgets.Button.from_param(self.param[button], button_type=button_type, width=200)
        cancel_btn = pn.widgets.Button.from_param(self.param.cancel_btn, button_type='danger', width=200)

        code = 'btn.css_classes.push("pn-loading", "arc"); btn.properties.css_classes.change.emit(); ' \
               'other_btn.disabled=true;'
        action_btn.js_on_click(
            args={'btn': action_btn, 'other_btn': cancel_btn},
            code=code
        )
        cancel_btn.js_on_click(
            args={'other_btn': action_btn, 'btn': cancel_btn},
            code=code,
        )
        return pn.Row(action_btn, cancel_btn)

    def submit_view(self):
        self.is_submitable()
        return pn.Column(
            self.view,
            self.action_button,
            self.error_messages,
            name='Submit',
            sizing_mode='stretch_both',
        )

    def view(self):
        return pn.Param(self.param.job_name)

    def panel(self):
        return pn.Column(
            '# Submit Job',
            pn.widgets.Button.from_param(self.param.previous_btn, button_type='primary', width=100),

            pn.layout.Tabs(
                self.submit_view(),
                self.pbs_options_view(),
                self.advanced_options_view(),
                active=1,
                sizing_mode='stretch_both',
            ),
            sizing_mode='stretch_both',
        )
