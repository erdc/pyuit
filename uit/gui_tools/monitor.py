import logging

import param
import panel as pn

from .utils import PbsJobTabbedViewer

logger = logging.getLogger(__name__)


class HpcJobMonitor(PbsJobTabbedViewer):
    title = param.String(default="Job Status")
    next_btn = param.Action(lambda self: self.next(), label="Next")

    def next(self):
        self.ready = True

    @param.output(finished_job_ids=list)
    def finished_jobs(self):
        return self.status_tab.statuses[self.statuses["status"] == "F"]["job_id"].tolist()

    def header_panel(self):
        row = super().header_panel()
        row.extend(
            [
                pn.Param(
                    self.param.next_btn,
                    widgets={"next_btn": {"button_type": "success", "width": 100}},
                ),
            ]
        )
        return row
