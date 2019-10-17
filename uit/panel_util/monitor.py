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
    TERMINAL_STATUSES = ['F']

    def __init__(self, **params):
        super().__init__(**params)

    @param.output(finished_job_ids=list)
    def finished_jobs(self):
        return self.statuses[self.statuses['status'] == 'F']['job_id'].tolist()

    @param.depends('jobs', watch=True)
    def update_statuses(self):
        self.statuses = None
        self.statuses = PbsJob.update_statuses(self.jobs, as_df=True)
        objects = [j for j in self.jobs if j.status in self.TERMINAL_STATUSES]
        self.param.active_job.objects = objects
        self.param.active_job.names = {j.job_id: j for j in objects}
        if objects:
            self.active_job = objects[0]

    @param.depends('active_job')
    def out_log(self):
        job = self.active_job
        if job is not None:
            log = job.get_stdout_log()
            return pn.pane.Str(log, width=800)

    @param.depends('active_job')
    def err_log(self):
        job = self.active_job
        if job is not None:
            log = job.get_stderr_log()
            return pn.pane.Str(log, width=800)

    @param.depends('statuses')
    def statuses_panel(self):
        statuses = self.statuses \
            if self.statuses is not None \
            else 'https://upload.wikimedia.org/wikipedia/commons/2/2a/Loading_Key.gif'
        return pn.panel(statuses)

    def status_panel(self):
        return pn.Column(
            self.statuses_panel,
            pn.Row(self.param.update, width=100),
        )

    def logs_panel(self):
        return pn.Column(
            pn.Row(self.param.active_job, width=200),
            pn.layout.Tabs(pn.panel(self.out_log, name='Out Log'),
                           pn.panel(self.err_log, name='Err Log'),
                           )
        )

    def panel(self):
        return pn.Column(
            pn.pane.HTML('<h1>Job Status</h1>'),
            pn.layout.Tabs(
                pn.panel(self.status_panel, name='Status'),
                pn.panel(self.logs_panel, name='Logs'),
            ),
        )

