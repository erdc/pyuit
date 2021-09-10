from .connect import HpcAuthenticate, HpcConnect
from .submit import HpcSubmit
from .monitor import HpcJobMonitor
from .results import HpcJobResultsViewer
from .utils import (
    make_bk_label,
    HpcConfigurable,
    HpcWorkspaces,
    PbsJobTabbedViewer,
    TabView,
    LogsTab,
    FileViewerTab,
    StatusTab,
)
from .file_browser import (
    FileBrowser,
    FileManager,
    FileManagerHPC,
    FileTransfer,
    HpcFileBrowser,
    FileSelector,
    SelectFile,
    FileViewer,
)

import param
import panel as pn


pn.Column.param._add_parameter('visible', param.Boolean(default=True))
