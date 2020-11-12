import logging

import param
import panel as pn
from pkg_resources import resource_filename

from uit.uit import Client
from uit.job import PbsJob
from .file_browser import FileViewer

from .configurable import HpcConfigurable

log = logging.getLogger(__name__)


class HpcJobMonitor(HpcConfigurable):
    uit_client = param.ClassSelector(Client)
    jobs = param.List()
    update = param.Action(lambda self: self.update_statuses())
    statuses = param.DataFrame()
    selected_job = param.ObjectSelector(label='Job')
    active_sub_job = param.ObjectSelector(label='Iteration')
    log = param.ObjectSelector(objects=[], label='Log File')
    custom_logs = param.List(default=[])
    num_log_lines = param.Integer(default=100, label='n')
    file_viewer = param.ClassSelector(FileViewer)
    ready = param.Boolean()
    next_btn = param.Action(lambda self: self.next(), label='Next')
    terminate_btn = param.Action(lambda self: self.terminate_job(), label='Terminate')

    def __init__(self, **params):
        super().__init__(**params)
        self.tabs = [
            ('Status', self.status_panel),
            ('Logs', self.logs_panel),
            ('Files', self.file_browser_panel),
        ]

    def next(self):
        self.ready = True

    def terminate_job(self):
        self.selected_job.terminate()
        self.param.terminate_btn.constant = True

    @param.output(finished_job_ids=list)
    def finished_jobs(self):
        return self.statuses[self.statuses['status'] == 'F']['job_id'].tolist()

    @param.depends('jobs', watch=True)
    def update_selected_job(self):
        self.param.selected_job.names = {j.job_id: j for j in self.jobs}
        self.param.selected_job.objects = self.jobs
        self.selected_job = self.jobs[0] if self.jobs else None

    @param.depends('selected_job', watch=True)
    def update_statuses(self):
        self.statuses = None
        sub_jobs = self.selected_job.sub_jobs
        self.statuses = PbsJob.update_statuses(sub_jobs, as_df=True)
        objects = [j for j in sub_jobs if j.status != 'Q']
        self.param.active_sub_job.names = {j.job_id: j for j in objects}
        self.param.active_sub_job.objects = objects
        active_jobs = any([j for j in sub_jobs if j.status in ('Q', 'R')])
        self.param.terminate_btn.constant = not active_jobs
        if objects:
            self.active_sub_job = objects[0]

    @param.depends('active_sub_job', watch=True)
    def update_log(self):
        self.param.log.objects = ['stdout', 'stderr'] + [self.active_sub_job.resolve_path(p) for p in self.custom_logs]
        self.log = 'stdout'

    @param.depends('active_sub_job')
    def out_log(self):
        return self.get_log(lambda job: job.get_stdout_log())

    @param.depends('active_sub_job')
    def err_log(self):
        return self.get_log(lambda job: job.get_stderr_log())

    @param.depends('active_sub_job')
    def x_log(self, log_file):
        try:
            return self.get_log(lambda job: job.get_custom_log(log_file, num_lines=self.num_log_lines))
        except RuntimeError as e:
            log.exception(e)

    def get_log(self, func):
        job = self.active_sub_job
        if job is not None:
            log_contents = func(job)
            return pn.pane.Str(log_contents, width=800)

    @param.depends('statuses')
    def statuses_panel(self):
        statuses = pn.panel(self.statuses, width=1300) \
            if self.statuses is not None \
            else pn.pane.GIF(resource_filename('panel', 'assets/spinner.gif'))
        return statuses

    @param.depends('selected_job')
    def status_panel(self):
        if self.selected_job:
            return pn.Column(
                self.statuses_panel,
                pn.Param(self.param.update, widgets={'update': {'button_type': 'primary', 'width': 100}}),
            )
        else:
            return pn.pane.HTML('<h2>No jobs are available</h2>')

    @param.depends('active_sub_job', 'log')
    def log_pane(self):
        if self.log == 'stdout':
            return self.out_log()
        elif self.log == 'stderr':
            return self.err_log()
        else:
            return self.x_log(self.log)

    @param.depends('jobs')
    def logs_panel(self):
        return pn.Column(
            pn.Param(self, parameters=['active_sub_job', 'log'], show_name=False, width=300),
            self.log_pane,
        )

    @param.depends('uit_client', watch=True)
    def configure_file_viewer(self):
        self.file_viewer = FileViewer(uit_client=self.uit_client)
        self.file_viewer.configure_file_selector()

    @param.depends('selected_job')
    def file_browser_panel(self):
        viewer = self.file_viewer.panel if self.file_viewer else pn.Spacer()
        if self.selected_job is not None:
           self.file_viewer.file_path = str(self.selected_job.working_dir)
        return pn.Column(
            viewer,
            name='Files',
            width_policy='max',
        )

    def panel(self):
        return pn.Column(
            '# Job Status',
            pn.Row(
                pn.panel(self.param.selected_job, width_policy='max'),
                pn.Param(self.param.next_btn, widgets={'next_btn': {'button_type': 'success', 'width': 100}}),
                pn.Param(self.param.terminate_btn, widgets={'terminate_btn': {'button_type': 'danger', 'width': 100}}),
            ),
            pn.layout.Tabs(
                *self.tabs,
            ),
        )


class HpcJobListMonitor(HpcJobMonitor):
    jobs = param.List()

    @param.depends('jobs', watch=True)
    def update_selected_job(self):
        self.param.selected_job.names = {j.job_id: j for j in self.jobs}
        self.param.selected_job.objects = self.jobs
        self.selected_job = self.jobs[0] if self.jobs else None
