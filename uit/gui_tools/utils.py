from collections import OrderedDict
from pathlib import Path, PurePosixPath
import logging

import param
import panel as pn
import pandas as pd
import yaml

from .file_browser import FileViewer
from ..uit import Client
from ..job import PbsJob, PbsArrayJob

log = logging.getLogger(__name__)


class HpcConfigurable(param.Parameterized):
    configuration_file = param.String()
    uit_client = param.ClassSelector(Client)
    environment_variables = param.ClassSelector(OrderedDict, default=OrderedDict())
    modules_to_load = param.ListSelector(default=[])
    modules_to_unload = param.ListSelector(default=[])
    load_modules = param.List()
    unload_modules = param.List()

    @param.depends('uit_client', watch=True)
    def update_configurable_hpc_parameters(self):
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
                row = group.iloc[0]
                if group.shape[0] > 1:
                    row = group[group['Default']].iloc[0]
                module = f'{row.Name}/{row.Version}'
                modules.append(module)
            else:
                log.info(f'Module "{m}" is  invalid.')
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
    selected_job = param.ObjectSelector(default=None, label='Job')
    selected_sub_job = param.ObjectSelector(label='Experiment Point', precedence=0.1)
    active_job = param.Parameter()

    @property
    def run_dir(self):
        return self.active_job.run_dir

    @property
    def is_array(self):
        return isinstance(self.selected_job, PbsArrayJob)

    @param.depends('jobs', watch=True)
    def update_selected_job(self):
        self.param.selected_job.names = {j.job_id: j for j in self.jobs}
        self.param.selected_job.objects = self.jobs
        self.selected_job = self.jobs[0] if self.jobs else None
        self.param.selected_job.precedence = 1 if len(self.jobs) > 1 else -1

    @param.depends('selected_job', watch=True)
    def update_selected_job_dependencies(self):
        self.update_working_dir()
        self.update_selected_sub_job()
        self.update_active_job()

    def update_selected_sub_job(self):
        if self.is_array:
            objects = [j for j in self.selected_job.sub_jobs]
            self.param.selected_sub_job.names = {j.job_id: j for j in objects}
            self.param.selected_sub_job.objects = objects
            if objects:
                self.selected_sub_job = objects[0]

    def update_working_dir(self):
        if self.selected_job is not None:
            self.working_dir = self.selected_job.working_dir

    def update_active_job(self):
        if self.is_array:
            self.active_job = self.selected_sub_job
            self.param.selected_sub_job.precedence = 1
        else:
            self.active_job = self.selected_job
            self.param.selected_sub_job.precedence = -1

    @param.depends('selected_job')
    def header_panel(self):
        return pn.Row(pn.panel(self.param.selected_job, width_policy='max'))

    def panel(self):
        return pn.Column(
            f'# {self.title}',
            self.header_panel,
            pn.layout.Tabs(
                *self.tabs,
                sizing_mode='stretch_both',
            ),
            sizing_mode='stretch_both',
        )


class TabView(param.Parameterized):
    title = param.String()
    parent = param.ClassSelector(PbsJobTabbedViewer)

    @property
    def tab(self):
        return self.title, self.panel()

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
    custom_logs = param.List(default=[])
    num_log_lines = param.Integer(default=100, label='n')
    refresh_btn = param.Action(lambda self: self.param.trigger('log'), label='Refresh')

    def __init__(self, **params):
        super().__init__(**params)
        self.update_log()

    @param.depends('custom_logs', watch=True)
    def update_log(self):
        self.param.log.objects = ['stdout', 'stderr']
        self.log = 'stdout'
        if self.custom_logs:
            self.param.log.objects += self.custom_logs
            self.param.log.names = {cl.split('/')[-1]: cl for cl in self.custom_logs}

    def x_log(self, log_file):
        try:
            return self.get_log(lambda job: job.get_custom_log(log_file, num_lines=self.num_log_lines))
        except RuntimeError as e:
            log.exception(e)

    def get_log(self, func):
        job = self.active_job
        if job is not None:
            log_contents = func(job)
            return pn.pane.Str(log_contents, width=800)

    @param.depends('parent.active_job', 'log')
    def log_pane(self):
        spn = pn.widgets.indicators.LoadingSpinner(value=True, color='primary', aspect_ratio=1, width=0)
        refresh_btn = pn.Param(
            self.param.refresh_btn, widgets={'refresh_btn': {'button_type': 'primary', 'width': 100}}
        )[0]
        refresh_btn.js_on_click(args={'btn': refresh_btn, 'spn': spn}, code='btn.visible=false; spn.width=50;')
        if self.log == 'stdout':
            log_content = self.get_log(lambda job: job.get_stdout_log())
        elif self.log == 'stderr':
            log_content = self.get_log(lambda job: job.get_stderr_log())
        else:
            log_content = self.x_log(self.log)
        return pn.Column(
            refresh_btn, spn,
            log_content,
        )

    def panel(self):
        return pn.Column(
            pn.Param(self.parent.param.selected_sub_job, width=300),
            pn.Param(self.param.log, width=300),
            self.log_pane,
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

    def view(self):
        panel = self.file_viewer.panel()
        panel.min_height = 1000
        return panel

    def panel(self):
        return self.view


class StatusTab(TabView):
    title = param.String(default='Status')
    statuses = param.DataFrame(precedence=0.1)
    update = param.Action(lambda self: self.update_statuses(), precedence=0.2)
    terminate_btn = param.Action(lambda self: self.terminate_job(), label='Terminate', precedence=0.3)

    @param.depends('parent.selected_job', watch=True)
    def update_statuses(self):
        if self.selected_job is not None:
            if self.is_array:
                self.statuses = None
                jobs = [self.selected_job] + self.selected_job.sub_jobs
                self.statuses = PbsJob.update_statuses(jobs, as_df=True)
            else:
                self.statuses = self.uit_client.status(self.selected_job.job_id, as_df=True)
            self.update_terminate_btn()

    def terminate_job(self):
        self.selected_job.terminate()
        self.update_statuses()

    def update_terminate_btn(self):
        self.param.terminate_btn.constant = self.selected_job.status not in ('Q', 'R', 'B')

    @param.depends('statuses')
    def statuses_panel(self):
        if self.statuses is not None:
            return pn.Column(
                pn.Param(
                    self.param.statuses,
                    widgets={'statuses': {'show_index': False, 'width': 1300}},
                ),
                pn.Param(
                    self,
                    parameters=['update', 'terminate_btn'],
                    widgets={
                        'update': {'button_type': 'primary', 'width': 100},
                        'terminate_btn': {'button_type': 'danger', 'width': 100},
                    },
                    show_name=False,
                    default_layout=pn.Row,
                ),
                sizing_mode='stretch_width',
            )
        else:
            return pn.indicators.LoadingSpinner(value=True, color='primary', aspect_ratio=1, width=50)

    @param.depends('parent.selected_job')
    def status_panel(self):
        if self.selected_job:
            return self.statuses_panel
        else:
            return pn.pane.HTML('<h2>No jobs are available</h2>')

    def panel(self):
        return self.status_panel
