import logging

import param
import panel as pn

from uit.uit import Client
from uit.job import PbsJob

log = logging.getLogger(__name__)


class HpcJobMonitor(param.Parameterized):
    uit_client = param.ClassSelector(Client)
    jobs = param.List()
    update = param.Action(lambda self: self.update_statuses())
    statuses = param.DataFrame()
    active_job = param.ObjectSelector(label='Job')
    log = param.ObjectSelector(objects=[], label='Log File')
    custom_logs = param.List(default=[])
    num_log_lines = param.Integer(default=100, label='n')

    def __init__(self, **params):
        super().__init__(**params)
        self.tabs = [
            pn.panel(self.status_panel, name='Status'),
            pn.panel(self.logs_panel, name='Logs'),
        ]

    @param.output(finished_job_ids=list)
    def finished_jobs(self):
        return self.statuses[self.statuses['status'] == 'F']['job_id'].tolist()

    @param.depends('jobs', watch=True)
    def update_statuses(self):
        self.statuses = None
        self.statuses = PbsJob.update_statuses(self.jobs, as_df=True)
        objects = [j for j in self.jobs if j.status != 'Q']
        self.param.active_job.names = {j.job_id: j for j in objects}
        self.param.active_job.objects = objects
        if objects:
            self.active_job = objects[0]

    @param.depends('active_job', watch=True)
    def udpate_log(self):
        self.param.log.objects = ['stdout', 'stderr'] + [self.active_job.resolve_path(p) for p in self.custom_logs]
        self.log = 'stdout'

    @param.depends('active_job')
    def out_log(self):
        return self.get_log(lambda job: job.get_stdout_log())

    @param.depends('active_job')
    def err_log(self):
        return self.get_log(lambda job: job.get_stderr_log())

    @param.depends('active_job')
    def x_log(self, log_file):
        try:
            return self.get_log(lambda job: self.uit_client.call(f'tail -n {self.num_log_lines} {log_file}'))
        except RuntimeError as e:
            log.exception(e)

    def get_log(self, func):
        job = self.active_job
        if job is not None:
            log_contents = func(job)
            return pn.pane.Str(log_contents, width=800)

    @param.depends('statuses')
    def statuses_panel(self):
        statuses = self.statuses \
            if self.statuses is not None \
            else 'https://upload.wikimedia.org/wikipedia/commons/2/2a/Loading_Key.gif'
        return pn.panel(statuses)

    @param.depends('jobs')
    def status_panel(self):
        if self.jobs:
            return pn.Column(
                self.statuses_panel,
                pn.Row(self.param.update, width=100),
            )
        else:
            return pn.pane.HTML('<h2>No jobs are available</h2>')

    @param.depends('active_job', 'log')
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
            pn.Param(self, parameters=['active_job', 'log'], show_name=False, width=300),
            self.log_pane,
        )

    def panel(self):
        return pn.Column(
            pn.pane.HTML('<h1>Job Status</h1>'),
            pn.layout.Tabs(
                *self.tabs,
            ),
        )
