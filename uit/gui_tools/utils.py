from collections import OrderedDict
from pathlib import Path, PurePosixPath
import logging
import time

import param
import panel as pn
import pandas as pd
import yaml

from .file_browser import FileViewer
from ..uit import Client
from ..job import PbsJob, PbsArrayJob

logger = logging.getLogger(__name__)


def make_bk_label(label):
    return pn.pane.HTML(f'<div class="bk bk-input-group"><label class="bk">{label}</label></div>')


class HpcConfigurable(param.Parameterized):
    configuration_file = param.String()
    uit_client = param.ClassSelector(Client)
    environment_variables = param.ClassSelector(OrderedDict, default=OrderedDict())
    modules_to_load = param.ListSelector(default=[])
    modules_to_unload = param.ListSelector(default=[])

    @param.depends('uit_client', watch=True)
    def update_configurable_hpc_parameters(self, reset=False):
        if not (self.uit_client and self.uit_client.connected):
            return

        self.load_config_file(reset=reset)
        self.param.modules_to_unload.objects = sorted(self.uit_client.get_loaded_modules())
        self.param.modules_to_load.objects = self._get_modules_available_to_load()
        self.modules_to_load = self._validate_modules(self.param.modules_to_load.objects, self.modules_to_load)
        self.modules_to_unload = self._validate_modules(self.param.modules_to_unload.objects, self.modules_to_unload)

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
                row = group.iloc[0]
                if group.shape[0] > 1:
                    row = group[group['Default']].iloc[0]
                module = f'{row.Name}/{row.Version}'
                modules.append(module)
            else:
                logger.info(f'Module "{m}" is  invalid.')
        return sorted(modules)

    def load_config_file(self, reset=False):
        config_file = Path(self.configuration_file)
        if config_file.is_file():
            with config_file.open() as f:
                config = yaml.safe_load(f).get(self.uit_client.system, {})
            modules = config.get('modules')
            if modules:
                self.modules_to_load = self.modules_to_load or modules.get('load')
                self.modules_to_unload = self.modules_to_unload or modules.get('unload')
            if reset:
                self.environment_variables = OrderedDict(config.get('environment_variables'))
            else:
                self.environment_variables = self.environment_variables or OrderedDict(config.get('environment_variables'))


class HpcWorkspaces(HpcConfigurable):
    working_dir = param.ClassSelector(PurePosixPath)
    _user_workspace = param.ClassSelector(Path)

    @property
    def remote_workspace_suffix(self):
        try:
            return self.working_dir.relative_to(self.uit_client.WORKDIR)
        except ValueError:
            return self.working_dir.relative_to('/p')

    @property
    def workspace(self):
        return self.user_workspace / self.remote_workspace_suffix

    @property
    def user_workspace(self):
        if self._user_workspace is None:
            self._user_workspace = Path('workspace', self.uit_client.username)
            self._user_workspace.mkdir(parents=True, exist_ok=True)
        return self._user_workspace


class PbsJobTabbedViewer(HpcWorkspaces):
    title = param.String()
    jobs = param.List()
    tabs = param.List()
    selected_job = param.ObjectSelector(default=None, label='Job')
    selected_sub_job = param.ObjectSelector(label='Experiment Point', precedence=0.1)
    active_job = param.Parameter()
    custom_logs = param.List(default=[])
    ready = param.Boolean()
    disable_status_update = param.Boolean()

    def __init__(self, **params):
        super().__init__(**params)
        if self.jobs:
            self.update_selected_job()
        self.status_tab = StatusTab(parent=self, disable_update=self.disable_status_update)
        self.logs_tab = LogsTab(parent=self, custom_logs=self.custom_logs)
        self.files_tab = FileViewerTab(parent=self)
        self.tabs = self.default_tabs = [
            self.status_tab.tab,
            self.logs_tab.tab,
            self.files_tab.tab,
        ]

    def __str__(self):
        return f'<{self.__class__.__name__} job={self.selected_job}>'

    def __repr__(self):
        return self.__str__()

    @property
    def run_dir(self):
        return self.active_job.run_dir

    @property
    def is_array(self):
        return isinstance(self.selected_job, PbsArrayJob)

    @param.depends('jobs', watch=True)
    def update_selected_job(self):
        if not self.environment_variables:
            self.update_configurable_hpc_parameters()
        self.param.selected_job.names = {j.job_id: j for j in self.jobs}
        self.param.selected_job.objects = self.jobs
        self.selected_job = self.jobs[0] if self.jobs else None
        self.param.selected_job.precedence = 1 if len(self.jobs) > 1 else -1

    @param.depends('selected_job', watch=True)
    def update_selected_job_dependencies(self):
        self.update_working_dir()
        if self.is_array:
            self.update_selected_sub_job()
            self.param.selected_sub_job.precedence = 1
        else:
            self.active_job = self.selected_job
            self.param.selected_sub_job.precedence = -1

    def update_selected_sub_job(self):
        objects = [j for j in self.selected_job.sub_jobs]
        self.param.selected_sub_job.names = {j.job_id: j for j in objects}
        self.param.selected_sub_job.objects = objects
        if objects:
            self.selected_sub_job = objects[0]

    def update_working_dir(self):
        if self.selected_job is not None:
            self.working_dir = self.selected_job.working_dir

    @param.depends('selected_sub_job', watch=True)
    def update_active_job(self):
        if self.is_array:
            self.active_job = self.selected_sub_job

    @param.depends('selected_job')
    def header_panel(self):
        return pn.Row(pn.panel(self.param.selected_job, width_policy='max'))

    @param.depends('tabs')
    def tabs_panel(self):
        return pn.layout.Tabs(
            *self.tabs,
            sizing_mode='stretch_both',
        )

    def panel(self):
        return pn.Column(
            f'# {self.title}',
            self.header_panel,
            self.tabs_panel,
            sizing_mode='stretch_both',
        )


class TabView(param.Parameterized):
    title = param.String()
    parent = param.ClassSelector(PbsJobTabbedViewer)

    def __str__(self):
        return f'<{self.__class__.__name__}: {self.title} parent={self.parent}'

    def __repr__(self):
        return self.__str__()

    @property
    def tab(self):
        return self.title, self.panel

    @property
    def uit_client(self):
        return self.parent.uit_client

    @property
    def working_dir(self):
        return self.parent.working_dir

    @property
    def run_dir(self):
        return self.parent.run_dir

    @property
    def workspace(self):
        return self.parent.workspace

    @property
    def selected_job(self):
        return self.parent.selected_job

    @property
    def selected_sub_job(self):
        return self.parent.selected_sub_job

    @property
    def active_job(self):
        return self.parent.active_job

    @property
    def is_array(self):
        return self.parent.is_array

    def panel(self):
        pass


class LogsTab(TabView):
    title = param.String(default='Logs')
    log = param.ObjectSelector(objects=[], label='Log File', precedence=0.2)
    log_content = param.String()
    custom_logs = param.List(default=[])
    num_log_lines = param.Integer(default=100, label='n')
    refresh_btn = param.Action(lambda self: self.param.trigger('log'), label='Refresh')

    def __init__(self, **params):
        super().__init__(**params)
        self.update_log()
        self.get_log()

    @param.depends('custom_logs', watch=True)
    def update_log(self):
        self.param['log'].objects = ['stdout', 'stderr']
        self.log = 'stdout'
        if self.custom_logs:
            self.param['log'].objects += self.custom_logs
            self.param['log'].names = {cl.split('/')[-1]: cl for cl in self.custom_logs}

    @param.depends('parent.active_job', 'log', watch=True)
    def get_log(self):
        job = self.active_job
        if job is not None and self.log is not None:
            if self.log == 'stdout':
                log_content = job.get_stdout_log(bytes=100_000, start_from=-100_000)
            elif self.log == 'stderr':
                log_content = job.get_stderr_log(bytes=100_000, start_from=-100_000)
            else:
                try:
                    log_content = job.get_custom_log(self.log, num_lines=self.num_log_lines)
                except RuntimeError as e:
                    logger.exception(e)
            if self.log_content == log_content:
                self.param.trigger('log_content')
            else:
                self.log_content = log_content

    @param.depends('log_content')
    def panel(self):
        log_content = pn.widgets.Ace.from_param(
            self.param.log_content,
            readonly=True, theme='monokai', sizing_mode='stretch_both', min_height=500
        )

        refresh_btn = pn.widgets.Button.from_param(self.param.refresh_btn, button_type='primary', width=100)
        args = {'log': log_content, 'btn': refresh_btn}
        code = 'btn.css_classes.push("pn-loading", "arc"); btn.properties.css_classes.change.emit(); ' \
               'log.css_classes.push("pn-loading", "arc"); log.properties.css_classes.change.emit();'
        refresh_btn.js_on_click(args=args, code=code)

        if self.is_array:
            sub_job_selector = pn.widgets.Select.from_param(self.parent.param.selected_sub_job, width=300)
            sub_job_selector.jscallback(args=args, value=code)
        else:
            sub_job_selector = None

        log_type_selector = pn.widgets.Select.from_param(self.param['log'], width=300)
        log_type_selector.jscallback(args, value=code)

        return pn.Column(
            sub_job_selector,
            log_type_selector,
            refresh_btn,
            log_content.param.theme,
            log_content,
            sizing_mode='stretch_both'
        )


class FileViewerTab(TabView):
    title = param.String(default='Files')
    file_viewer = param.ClassSelector(FileViewer, default=FileViewer())

    def __init__(self, **params):
        super().__init__(**params)
        self.configure_file_viewer()

    @param.depends('parent.uit_client', watch=True)
    def configure_file_viewer(self):
        if self.uit_client is not None:
            self.file_viewer.uit_client = self.uit_client

    @param.depends('parent.selected_job', watch=True)
    def update_file_path(self):
        if self.file_viewer:
            self.file_viewer.file_path = str(self.selected_job.run_dir)

    def panel(self):
        panel = self.file_viewer.panel()
        panel.min_height = 1000
        return panel


class StatusTab(TabView):
    title = param.String(default='Status')
    statuses = param.DataFrame(precedence=0.1)
    update_status = param.Action(lambda self: self.update_statuses(update_cache=True), precedence=0.2)
    terminate_btn = param.Action(lambda self: None, label='Terminate', precedence=0.3)
    yes_btn = param.Action(lambda self: self.terminate_job(), label='Yes', precedence=0.4)
    cancel_btn = param.Action(lambda self: None, label='Cancel', precedence=0.5)
    disable_update = param.Boolean()

    @param.depends('parent.selected_job', watch=True)
    def update_statuses(self, update_cache=False):
        if self.selected_job is not None:
            if update_cache:
                self.selected_job.update_status()
                self.update_terminate_btn()
            qstat = self.selected_job.qstat
            if qstat is None:
                statuses = None
            elif self.is_array:
                statuses = pd.DataFrame.from_dict(qstat).T
            else:
                statuses = pd.DataFrame(qstat, index=[0])
            if statuses is not None:
                statuses.set_index('job_id', inplace=True)
                statuses = statuses[['username', 'queue', 'jobname', 'session_id', 'nds', 'tsk', 'requested_memory', 'requested_time', 'status', 'elapsed_time']]
            self.statuses = statuses

    def terminate_job(self):
        self.selected_job.terminate()
        time.sleep(10)
        self.update_statuses()

    def update_terminate_btn(self):
        self.param.terminate_btn.constant = self.selected_job.status not in ('Q', 'R', 'B')

    @param.depends('statuses')
    def statuses_panel(self):
        statuses_table = pn.widgets.DataFrame.from_param(self.param.statuses, width=1300) \
            if self.statuses is not None else pn.pane.Alert('No status information available.', alert_type='info')

        if self.disable_update:
            buttons = None
        else:
            update_btn = pn.widgets.Button.from_param(self.param.update_status, button_type='primary', width=100)
            terminate_btn = pn.widgets.Button.from_param(self.param.terminate_btn, button_type='danger', width=100)
            yes_btn = pn.widgets.Button.from_param(self.param.yes_btn, button_type='danger', width=100)
            cancel_btn = pn.widgets.Button.from_param(self.param.cancel_btn, button_type='success', width=100)

            yes_btn.visible = False
            cancel_btn.visible = False

            msg = pn.indicators.String(
                value='Are you sure you want to terminate the job. This cannot be undone.',
                css_classes=['bk', 'alert', 'alert-danger'], default_color='inherit', font_size='inherit',
                visible=False,
            )

            terminate_confirmation = pn.Column(
                msg, pn.Row(yes_btn, cancel_btn, margin=20), background='#ffffff',
            )

            args = {'update_btn': update_btn, 'terminate_btn': terminate_btn, 'statuses_table': statuses_table,
                    'msg': msg, 'yes_btn': yes_btn, 'cancel_btn': cancel_btn, 'term_col': terminate_confirmation}
            terminate_code = 'update_btn.disabled=true; terminate_btn.visible=false; ' \
                             'msg.visible=true; yes_btn.visible=true; cancel_btn.visible=true; ' \
                             'term_col.css_classes=["panel-widget-box"]'
            cancel_code = 'update_btn.disabled=false; terminate_btn.visible=true; ' \
                          'msg.visible=false; yes_btn.visible=false; cancel_btn.visible=false; term_col.css_classes=[]'

            terminate_btn.js_on_click(args=args, code=terminate_code)
            cancel_btn.js_on_click(args=args, code=cancel_code)

            code = (
                'btn.css_classes.push("pn-loading", "arc"); '
                'btn.properties.css_classes.change.emit(); '
                'other_btn.disabled=true; '
                'statuses_table.css_classes.push("pn-loading", "arc"); '
                'statuses_table.properties.css_classes.change.emit();'
            )

            update_btn.js_on_click(
                args={'btn': update_btn, 'other_btn': terminate_btn, 'statuses_table': statuses_table},
                code=code,
            )
            yes_btn.js_on_click(
                args={'btn': terminate_btn, 'other_btn': update_btn, 'statuses_table': statuses_table},
                code=code,
            )

            buttons = pn.Row(update_btn, terminate_btn, terminate_confirmation)

        return pn.Column(
            statuses_table,
            buttons,
            sizing_mode='stretch_width',
        )

    @param.depends('parent.selected_job')
    def panel(self):
        if self.selected_job:
            return self.statuses_panel
        else:
            return pn.pane.HTML('<h2>No jobs are available</h2>')
