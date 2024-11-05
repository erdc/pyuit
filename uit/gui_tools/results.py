import logging

import param

from .utils import PbsJobTabbedViewer

logger = logging.getLogger(__name__)


class HpcJobResultsViewer(PbsJobTabbedViewer):
    title = param.String(default="View Results")
    disable_status_update = param.Boolean(default=True)
