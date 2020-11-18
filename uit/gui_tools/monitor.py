import logging

import param
import panel as pn

from .utils import PbsJobTabbedViewer, StatusTab, LogsTab, FileViewerTab

log = logging.getLogger(__name__)


class HpcJobMonitor(PbsJobTabbedViewer):
    title = param.String(default='Job Status')
    custom_logs = param.List(default=[])
    ready = param.Boolean()
    next_btn = param.Action(lambda self: self.next(), label='Next')
    terminate_btn = param.Action(lambda self: self.terminate_job(), label='Terminate')

    def __init__(self, **params):
        super().__init__(**params)
        self.status_tab = StatusTab(parent=self)
        self.logs_tab = LogsTab(parent=self, custom_logs=self.custom_logs)
        self.files_tab = FileViewerTab(parent=self)
        self.tabs = self.default_tabs = [
            self.status_tab.tab,
            self.logs_tab.tab,
            self.files_tab.tab,
        ]

    def next(self):
        self.ready = True

    def terminate_job(self):
        self.selected_job.terminate()
        self.param.terminate_btn.constant = True

    @param.depends('selected_job', watch=True)
    def update_terminate_btn(self):
        self.param.terminate_btn.constant = self.selected_job.status not in ('Q', 'R')

    @param.output(finished_job_ids=list)
    def finished_jobs(self):
        return self.status_tab.statuses[self.statuses['status'] == 'F']['job_id'].tolist()

    def header_panel(self):
        row = super().header_panel()
        row.extend([
            pn.Param(self.param.next_btn, widgets={'next_btn': {'button_type': 'success', 'width': 100}}),
            pn.Param(self.param.terminate_btn, widgets={'terminate_btn': {'button_type': 'danger', 'width': 100}}),
        ])
        return row
