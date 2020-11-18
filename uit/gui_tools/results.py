import logging

import param

from .utils import PbsJobTabbedViewer, LogsTab, FileViewerTab

log = logging.getLogger(__name__)


class HpcJobResultsViewer(PbsJobTabbedViewer):
    title = param.String(default='View Results')
    custom_logs = param.List(default=[])
    ready = param.Boolean()
    next_btn = param.Action(lambda self: self.next(), label='Next')

    def __init__(self, **params):
        super().__init__(**params)
        self.logs_tab = LogsTab(parent=self, custom_logs=self.custom_logs)
        self.files_tab = FileViewerTab(parent=self)
        self.tabs = self.default_tabs = [
            self.logs_tab.tab,
            self.files_tab.tab,
        ]
