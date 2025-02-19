import os
import glob
import logging
from pathlib import Path, PurePosixPath
from functools import wraps

import param
import panel as pn
from panel.viewable import Viewer
import panel.models.ace  # noqa: F401

from uit import Client
from uit.async_client import AsyncClient

logger = logging.getLogger(__name__)


def get_js_loading_code(obj):
    return f'{obj}.css_classes.push("pn-loading", "pn-arc"); {obj}.properties.css_classes.change.emit();'  # noqa


class FileManager(param.Parameterized):
    directory = param.String(default=os.getcwd(), precedence=0.1)
    file_keyword = param.String(doc="Keyword for file name. Hidden from ui.", default="*", precedence=-1)

    def __init__(self, **params):
        super().__init__(**params)
        self.cross_selector = pn.widgets.CrossSelector(name="Files", value=[], options=[], width=900)
        self._update_files()

    @property
    def value(self):
        return self.cross_selector.value

    @param.depends("directory", watch=True)
    def _update_files(self):
        self.cross_selector.options = glob.glob(os.path.join(self.directory, "*" + self.file_keyword + "*"))

    def panel(self):
        return pn.Column(self.param.directory, self.cross_selector, width=700)

    @param.output(selected_files=list)
    def output(self):
        """Return a list of the values in the right hand box"""
        return self.cross_selector.value


class FileManagerHPC(FileManager):
    """File manager for HPC applications using the uit client for communication.

    This extension of FileManager does not currently make use of
    FileManager.root or FileManager.file_keyword

    """

    uit_client = param.ClassSelector(class_=Client, precedence=-1)

    @param.depends("uit_client", watch=True)
    def _initialize_directory(self):
        if self.uit_client:
            self.directory = str(self.uit_client.WORKDIR)
            self._update_files()

    @param.depends("directory", watch=True)
    def _update_files(self):
        if self.uit_client:
            # get the ls from the client
            ls_df = self.uit_client.list_dir(path=self.directory, parse=True, as_df=True)

            # catch for errors returned as dict
            if type(ls_df) is dict:
                raise RuntimeError(
                    f"""
                Request for directory list returned the error: {ls_df['error']}

                Directory requested: {self.directory}

                """
                )

            # convert dataframe to file list
            self.file_list = ls_df["path"].to_list()

            # update cross selector widget
            self.cross_selector.options = self.file_list

    def panel(self):
        return pn.Column(self.param.directory, self.cross_selector, width=700)


class FileTransfer(param.Parameterized):
    uit_client = param.ClassSelector(class_=Client, precedence=-1)

    from_location = param.Selector(default="local", objects=["local"], precedence=0.21)
    from_directory = param.String(precedence=0.22)
    to_location = param.Selector(default="local", objects=["local"], precedence=0.31)
    to_directory = param.String(precedence=0.32)
    file_manager = param.ClassSelector(class_=FileManager, precedence=0.4)
    transfer_button = param.Action(
        lambda self: self.param.trigger("transfer_button"),
        label="Transfer",
        precedence=1.0,
    )

    def __init__(self, uit_client, **params):

        super().__init__(**params)
        self.uit_client = uit_client or Client()
        self.file_manager = FileManagerHPC(uit_client=self.uit_client)

        # adjust to/from based on uit_client
        self.param.from_location.objects = [self.uit_client.system, "local"]
        self.from_location = self.uit_client.system
        self.param.to_location.objects = [self.uit_client.system, "local"]
        self.to_location = self.uit_client.system

    @param.depends("transfer_button", watch=True)
    def transfer(self):
        if self.from_location == "local":
            for local_file in self.file_manager.cross_selector.value:
                self.uit_client.put_file(local_file, self.to_directory)
        elif self.to_location == "local":
            for remote_file in self.file_manager.cross_selector.value:
                logger.info("transferring {}".format(remote_file))
                self.uit_client.get_file(
                    remote_file,
                    local_path=os.path.join(self.to_directory, os.path.basename(remote_file)),
                )

        else:
            logger.warning("HPC to HPC transfers are not supported.")

    @param.depends("from_directory", watch=True)
    def _update_file_manager(self):
        """ """
        self.file_manager.directory = self.from_directory

    def _from_location(self):
        return pn.Column(self.param.from_location, self.param.from_directory)

    @param.depends("from_location", watch=True)
    def _to_location(self):
        remote_dir = str(self.uit_client.WORKDIR)
        local_dir = os.getcwd()

        if self.from_location == "local":
            # set from location and dir
            self.from_directory = local_dir

            # set to location and dir
            self.to_location = self.uit_client.system
            self.to_directory = remote_dir

            # set file manager to local manager
            self.file_manager = FileManager()
        else:
            # set to location and dir
            self.to_location = "local"
            self.to_directory = local_dir
            self.from_directory = remote_dir

            # set file manager to hpc manager
            self.file_manager = FileManagerHPC(uit_client=self.uit_client)

        # set cross selector directory
        self.file_manager._update_files()

    @param.depends("from_directory", watch=True)
    def panel(self):
        from_box = pn.WidgetBox(pn.Column(self._from_location, pn.Column(self.file_manager.cross_selector)))

        to_box = pn.WidgetBox(
            pn.Column(self.param.to_location, self.param.to_directory),
            width=900,
            width_policy="max",
        )

        return pn.Column(from_box, to_box, pn.Param(self.param.transfer_button))


class FileBrowser(Viewer):
    """ """

    path = param.ClassSelector(class_=Path, precedence=-1)
    path_text = param.String(label="", precedence=0.3)
    home = param.Action(lambda self: self.go_home(), label="🏠", doc="Home", precedence=0.1)
    up = param.Action(lambda self: self.move_up(), label="⬆️", doc="Move Up", precedence=0.2)
    # refresh_controll triggers rather than calling validate directly to allow an async override of the validate method
    refresh_control = param.Action(
        lambda self: self.param.trigger("refresh_control"), label="🔄", doc="Refresh", precedence=0.25
    )
    callback = param.Action(lambda x: None, precedence=-1)
    file_listing = param.ListSelector(default=[], label="Single click to select a file or directory:", precedence=0.5)
    patterns = param.List(precedence=-1, default=["*"])
    show_hidden = param.Boolean(default=False, label="Show Hidden Files", precedence=0.35)
    _disabled = param.Boolean(default=False, precedence=-1)
    visible = param.Boolean(default=True, precedence=-1)
    _init = param.Parameter()

    def __init__(self, delayed_init=False, disabled=False, **params):
        self.delayed_init = delayed_init  # delayed init may be obsolete with async
        super().__init__(**params)
        self.file_listing_widget = None
        self.param.trigger("_init")
        self.disabled = disabled
        self.file_listing_widget = pn.widgets.MultiSelect.from_param(
            self.param.file_listing,
            height=200,
            width_policy="max",
            stylesheets=[
                ".bk-input option:hover { background-color: var(--design-surface-color, var(--primary-bg-subtle)); }"
            ],
        )
        show_hidden_widget = pn.widgets.Checkbox.from_param(self.param.show_hidden)
        widgets = pn.Param(
            self,
            parameters=self.controls + ["path_text"],
            widgets=self.control_styles,
            show_name=False,
        )[:]
        args = {"listing": self.file_listing_widget}
        code = get_js_loading_code("listing")
        self.file_listing_widget.jscallback(args=args, value=code)
        for wg in widgets[:-1]:
            wg.js_on_click(args=args, code=code)
        widgets[-1].jscallback(args=args, value=code)
        self._layout = pn.Column(
            pn.Row(*widgets, sizing_mode="stretch_width", margin=0),
            show_hidden_widget,
            self.file_listing_widget,
            sizing_mode="stretch_width",
            margin=0,
        )
        self._widgets = [*widgets, self.file_listing_widget, show_hidden_widget]

    def init(self):
        self.delayed_init = False
        self._initialize_path()

    @param.depends("_init", watch=True)
    def _initialize_path(self):
        if self.delayed_init:
            return

        if self.path_text:
            self.validate()

        if not self.path:
            self.go_home()

    def _new_path(self, path):
        return Path(path)

    @property
    def disabled(self):
        return self._disabled

    @disabled.setter
    def disabled(self, disabled):
        for p in self.controls + ["file_listing", "path_text", "show_hidden"]:
            self.param[p].constant = disabled
        self._disabled = disabled

    @param.depends("visible", watch=True)
    def update_visible(self):
        for wg in self._widgets:
            wg.visible = self.visible

    @property
    def value(self):
        if self.file_listing:
            return [str(self.path / v) for v in self.file_listing]
        else:
            return [self.path.as_posix()]

    def stop_loading(self):
        if self.file_listing_widget is not None:
            self.file_listing_widget.css_classes = ["uit-loading"]
            self.file_listing_widget.css_classes = []

    def do_callback(self, changed=True):
        self.stop_loading()
        if self.callback:
            self.callback(changed)

    def go_home(self):
        self.path_text = Path.cwd().as_posix()
        self.file_listing = []

    def move_up(self):
        self.path_text = self.path.parent.as_posix()
        self.file_listing = []

    @param.depends("file_listing", watch=True)
    def move_down(self):
        if self.file_listing:
            filename = self.file_listing[0]
            fn = self.path / filename
            self.path_text = fn.as_posix()

    @param.depends("path_text", "refresh_control", watch=True)
    def validate(self):
        """Check that inputted path is valid - set validator accordingly"""
        path = self._new_path(self.path_text)
        if path and path.is_dir():
            with param.discard_events(self):
                self.path = path
                self.file_listing = []
                # since events are discarded the widget value must be set manually
                if self.file_listing_widget:
                    self.file_listing_widget.value = []
        elif path and path.is_file():
            with param.discard_events(self):
                self.path = path.parent
                self.file_listing = [path.name]
        else:
            logger.warning(f"Invalid Directory: {path}")
        self.make_options()

    @param.depends("show_hidden", watch=True)
    def make_options(self):
        selected = []
        try:
            selected = [p.name + "/" for p in self.path.glob("*") if p.is_dir()]
            for pattern in self.patterns:
                selected.extend([p.name for p in self.path.glob(pattern) if not p.is_dir()])
            if not self.show_hidden:
                selected = [p for p in selected if not str(p).startswith(".")]
        except Exception as e:
            logger.exception(str(e))

        self.param.file_listing.objects = sorted(selected)
        self.stop_loading()
        self.do_callback()

    @property
    def controls(self):
        return ["home", "up", "refresh_control"]

    @property
    def control_styles(self):
        styles = {c: {"width": 25} for c in self.controls}

        styles.update(
            path_text={"width_policy": "max"},
        )
        return styles

    def __panel__(self):
        return self._layout

    def panel(self):
        from warnings import warn

        warn(
            f"The panel {self.__class__.__name__}.method is now deprecated. The object itself is now viewable.",
            DeprecationWarning,
        )
        return self._layout


class HpcPath(Path, PurePosixPath):
    """PurePath subclass that can make some system calls on an HPC system."""

    _has_init = hasattr(Path, "_init")  # i.e. Python 3.7
    _has_from_parts = hasattr(Path, "_from_parts")  # i.e. Python < 3.12

    def __init__(self, *args, is_dir=None, uit_client=None):
        if self._has_from_parts:
            super().__init__()
        else:
            super().__init__(*args)
        self.__initialize__(is_dir=is_dir, uit_client=uit_client)

    def __initialize__(self, is_dir=None, uit_client=None):
        self._is_dir = is_dir
        self.uit_client = uit_client
        self._is_file = None if is_dir is None else not is_dir
        self._ls = None

    def _init(self, template=None, is_dir=None, uit_client=None):
        if self._has_init:
            super()._init(template=template)
        self.uit_client = uit_client
        self._is_dir = is_dir

    def __new__(cls, *args, is_dir=None, uit_client=None):
        if cls._has_from_parts:
            if cls._has_init:
                self = cls._from_parts(args, init=False)
            else:
                self = cls._from_parts(args)
        else:
            self = super().__new__(cls, *args)
        self._init(is_dir=is_dir, uit_client=uit_client)
        return self

    def __truediv__(self, key):
        new_path = super().__truediv__(key)
        new_path.__initialize__(uit_client=self.uit_client)
        return new_path

    def _ensure_connected(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            if self.uit_client and self.uit_client.connected:
                return method(self, *args, **kwargs)
            logger.warning("Path has no uit client, or it is not connected!")

        return wrapper

    @property
    def ls(self):
        if self._ls is None:
            self._get_metadata()
        return self._ls

    @property
    def parent(self):
        parent = super().parent
        parent.__initialize__(is_dir=True, uit_client=self.uit_client)
        return parent

    @_ensure_connected
    def _get_metadata(self):
        if not self.is_absolute():
            self._str = str(self.uit_client.HOME / self)

        if self.name == "":
            # If I don't have a name, don't consider myself a directory or file.
            # Without this, the first directory chosen would break with a grayed-out file listing.
            # It has something to do with loading the home directory for the first time.
            return

        self._is_dir = True
        self._is_file = False
        self._ls = self.uit_client.list_dir(self.as_posix())
        if self._ls.get("success") != "true":
            # This could be because path is a file or because UIT+ returns an error.
            # Errors can be caused by broken symlinks inside the directory.
            # Try our own 'ls -l'
            self._is_dir = False
            self._ls = self.parse_list_dir(self.as_posix())

    def parse_list_dir(self, base_path):
        TYPES = {"d": "dir", "-": "file", "l": "link", "s": "dir"}
        parsed_ls = {"path": base_path, "dirs": [], "files": [], "links": []}
        ls = self.uit_client.call(f"ls -lL {base_path}", full_response=True)
        for f in ls["stdout"].splitlines()[1:]:
            parts = f.split()

            # handle case where group name contains a space
            try:
                int(parts[4])
            except ValueError:
                parts[3] += f" {parts.pop(4)}"

            try:
                perms, _, owner, group, size, mon, day, time, filename = parts[:9]
                metadata = {
                    "owner": owner,
                    "path": f"{base_path}/{filename}",
                    "size": int(size),
                    "lastmodified": f"{mon} {day} {time}",
                    "name": filename,
                    "perms": perms,
                    "type": TYPES[perms[0]],
                    "group": group,
                }
            except Exception as e:
                logger.warning(f'There was an error parsing an HPC file listing for line: "{f}"\n\n{e}')
                continue
            if perms.startswith("l"):
                metadata["link"] = parts[-1]
            parsed_ls[metadata["type"] + "s"].append(metadata)
        return parsed_ls

    def is_dir(self):
        if self._is_dir is None:
            self._get_metadata()
        return self._is_dir

    def is_file(self):
        if self._is_file is None:
            self._get_metadata()
        return self._is_file

    def _get_file_list(self, file_meta_list, is_dir):
        file_list = list()
        for p in file_meta_list:
            file_path = p["path"].rsplit("/")[0] + f'/{p["name"]}'
            cls = self.__class__
            file_list.append(cls(file_path, is_dir=is_dir, uit_client=self.uit_client))
        return file_list

    def glob(self, pattern):
        result = list()
        result.extend(self._get_file_list(self.ls["dirs"], is_dir=True))
        result.extend(self._get_file_list(self.ls["files"], is_dir=False))
        return [r for r in result if r.match(pattern)]

    def exists(self):
        try:
            return self.is_dir() or self.is_file()
        except ValueError:
            return False


class AsyncHpcPath(HpcPath):
    @property
    async def ls(self):
        if self._ls is None:
            await self._get_metadata()
        return self._ls

    @HpcPath._ensure_connected
    async def _get_metadata(self):
        if not self.is_absolute():
            self._str = str(self.uit_client.HOME / self)

        if self.name == "":
            # If I don't have a name, don't consider myself a directory or file.
            # Without this, the first directory chosen would break with a grayed-out file listing.
            # It has something to do with loading the home directory for the first time.
            return

        self._is_dir = True
        self._is_file = False
        self._ls = await self.uit_client.list_dir(self.as_posix())
        if self._ls.get("success") != "true":
            # This could be because path is a file or because UIT+ returns an error.
            # Errors can be caused by broken symlinks inside the directory.
            # Try our own 'ls -l'
            self._is_dir = False
            self._ls = await self.parse_list_dir(self.as_posix())

    async def parse_list_dir(self, base_path):
        TYPES = {"d": "dir", "-": "file", "l": "link", "s": "dir"}
        parsed_ls = {"path": base_path, "dirs": [], "files": [], "links": []}
        ls = await self.uit_client.call(f"ls -lL {base_path}", full_response=True)
        for f in ls["stdout"].splitlines()[1:]:
            parts = f.split()

            # handle case where group name contains a space
            try:
                int(parts[4])
            except ValueError:
                parts[3] += f" {parts.pop(4)}"

            try:
                perms, _, owner, group, size, mon, day, time, filename = parts[:9]
                metadata = {
                    "owner": owner,
                    "path": f"{base_path}/{filename}",
                    "size": int(size),
                    "lastmodified": f"{mon} {day} {time}",
                    "name": filename,
                    "perms": perms,
                    "type": TYPES[perms[0]],
                    "group": group,
                }
            except Exception as e:
                logger.warning(f'There was an error parsing an HPC file listing for line: "{f}"\n\n{e}')
                continue
            if perms.startswith("l"):
                metadata["link"] = parts[-1]
            parsed_ls[metadata["type"] + "s"].append(metadata)
        return parsed_ls

    async def is_dir(self):
        if self._is_dir is None:
            await self._get_metadata()
        return self._is_dir

    async def is_file(self):
        if self._is_file is None:
            await self._get_metadata()
        return self._is_file

    async def glob(self, pattern):
        result = list()
        ls = await self.ls
        result.extend(self._get_file_list(ls["dirs"], is_dir=True))
        result.extend(self._get_file_list(ls["files"], is_dir=False))
        return [r for r in result if r.match(pattern)]

    async def exists(self):
        try:
            return await self.is_dir() or await self.is_file()
        except ValueError:
            return False


class HpcFileBrowser(FileBrowser):
    path = param.ClassSelector(class_=HpcPath)
    workdir = param.Action(lambda self: self.go_to_workdir(), label="⚙️", doc="Workdir", precedence=0.15)
    uit_client = param.ClassSelector(class_=Client)

    def __init__(self, uit_client, **params):
        params["uit_client"] = uit_client
        super().__init__(**params)

    @property
    def controls(self):
        controls = super().controls
        controls.insert(1, "workdir")
        return controls

    def _new_path(self, path):
        return HpcPath(path, uit_client=self.uit_client)

    @param.depends("uit_client", watch=True)
    def _initialize_path(self):
        super()._initialize_path()

    @HpcPath._ensure_connected
    def go_home(self):
        self.path_text = self._new_path(self.uit_client.HOME).as_posix()
        self.file_listing = []

    @HpcPath._ensure_connected
    def go_to_workdir(self):
        self.path_text = self._new_path(self.uit_client.WORKDIR).as_posix()
        self.file_listing = []


class AsyncHpcFileBrowser(HpcFileBrowser):
    uit_client = param.ClassSelector(class_=AsyncClient)

    def __init__(self, uit_client, **params):
        super().__init__(uit_client, **params)
        self.param.trigger("uit_client")  # to initialize asynchronously

    async def init(self):
        self.delayed_init = False
        await self._initialize_path()

    @param.depends("uit_client", watch=True)
    async def _initialize_path(self):
        if self.delayed_init:
            return

        if self.path_text:
            await self.validate()

        if not self.path:
            self.go_home()

    def _new_path(self, path):
        return AsyncHpcPath(path, uit_client=self.uit_client)

    @param.depends("path_text", "refresh_control", watch=True)
    async def validate(self):
        """Check that inputted path is valid - set validator accordingly"""
        path = self._new_path(self.path_text)
        if path and await path.is_dir():
            with param.discard_events(self):
                self.path = path
                self.file_listing = []
                # since events are discarded the widget value must be set manually
                if self.file_listing_widget:
                    self.file_listing_widget.value = []
        elif path and await path.is_file():
            with param.discard_events(self):
                self.path = path.parent
                self.file_listing = [path.name]
        else:
            logger.warning(f"Invalid Directory: {path}")
        await self.make_options()

    @param.depends("show_hidden", watch=True)
    async def make_options(self):
        selected = []
        try:
            selected = [p.name + "/" for p in await self.path.glob("*") if await p.is_dir()]
            for pattern in self.patterns:
                selected.extend([p.name for p in await self.path.glob(pattern) if not await p.is_dir()])
            if not self.show_hidden:
                selected = [p for p in selected if not str(p).startswith(".")]
        except Exception as e:
            logger.exception(str(e))

        self.param.file_listing.objects = sorted(selected)
        self.stop_loading()
        self.do_callback()


def create_file_browser(uit_client, **kwargs):
    if isinstance(uit_client, AsyncClient):
        return AsyncHpcFileBrowser(uit_client, **kwargs)
    if isinstance(uit_client, Client):
        return HpcFileBrowser(uit_client, **kwargs)


class FileSelector(Viewer):
    file_path = param.String(default="")
    show_browser = param.Boolean(default=False)
    browse_toggle = param.Action(lambda self: self.toggle(), label="📂", doc="Open file browser.")
    file_browser = param.ClassSelector(class_=FileBrowser)
    title = param.String(default="File Path")
    help_text = param.String()
    disabled = param.Boolean(precedence=-1)

    def __init__(self, disabled=False, **params):
        super().__init__(**params)
        self.file_browser_container = pn.Row(self.file_browser, sizing_mode="stretch_width", visible=False)
        self.file_browser = self.file_browser or FileBrowser(delayed_init=True)
        self.update_file(True)
        self.disabled = disabled
        self.param.file_path.label = self.title
        self.param.file_path.doc = self.help_text
        self._layout = pn.Column(
            self.input_row,
            self.file_browser_container,
            sizing_mode="stretch_width",
        )

    @param.depends("title", watch=True)
    def update_title(self):
        self.param.file_path.label = self.title

    @param.depends("help_text", watch=True)
    def update_help_text(self):
        self.param.file_path.doc = self.help_text

    @param.depends("disabled", watch=True)
    def update_disabled(self):
        for p in ["file_path", "browse_toggle"]:
            self.param[p].constant = self.disabled
        self.file_browser.disabled = self.disabled

    @param.depends("file_browser", watch=True)
    def update_callback(self):
        self.file_browser_container[0] = self.file_browser
        self.file_browser.callback = self.update_file

    def update_file(self, new_selection):
        if new_selection:
            if self.file_path == self.file_browser.path_text:
                self.param.trigger("file_path")
            else:
                self.file_path = self.file_browser.path_text

    def toggle(self):
        self.show_browser = not self.show_browser

    @param.depends("file_path", watch=True)
    def initialize_file_browser(self):
        if self.show_browser:
            self.file_browser.path_text = self.file_path

    @param.depends("show_browser", watch=True)
    def show_hide_browser(self):
        self.file_browser_container.visible = self.show_browser
        self.param.browse_toggle.label = "📂"
        self.param.browse_toggle.doc = "Open file browser"
        if self.show_browser:
            self.initialize_file_browser()
            self.param.browse_toggle.label = "❌"
            self.param.browse_toggle.doc = "Close file browser"

    def input_row(self):
        file_path = pn.widgets.TextInput.from_param(self.param.file_path, sizing_mode="stretch_width")
        browse_toggle = pn.widgets.Button.from_param(self.param.browse_toggle, width=40, align="end")

        browse_toggle.js_on_click(
            args={"btn": browse_toggle},
            code=get_js_loading_code("btn"),
        )

        return pn.Row(file_path, browse_toggle, width_policy="max", margin=0)

    @property
    def panel(self):
        return self._layout

    def __panel__(self):
        return self._layout


class FileViewer(Viewer):
    update_btn = param.Action(lambda self: self.param.trigger("update_btn"), label="Update", precedence=5)
    n = param.Integer(
        default=500,
        bounds=(0, 10_000),
        precedence=2,
        doc="Number of lines to show.",
    )
    cmd = param.ObjectSelector(
        default="head",
        objects=["head", "tail", "grep"],
        label="Command",
        precedence=1,
        doc="Command to execute to retrieve contents from selected file.",
    )
    line_wrap = param.Boolean(
        label="Line Wrap",
        precedence=3,
        doc='Set a maximum line length ("Wrap Length"). Lines that exceed "Wrap Length" will be broken up.',
    )
    wrap_length = param.Integer(
        default=100,
        label="Wrap Length",
        precedence=-4,
        doc="Maximum line length. Lines logger than specified length will be broken up.",
    )
    file_select = param.ClassSelector(class_=FileSelector)
    file_path = param.String()
    file_contents = param.String()
    uit_client = param.ClassSelector(class_=Client)
    grep_pattern = param.String(label="Search", precedence=-1)
    grep_context = param.Integer(label="Context Lines", default=4, precedence=-1)
    grep_ignore_case = param.Boolean(label="Ignore Case", precedence=-1)
    file_browser_class = HpcFileBrowser

    def __init__(self, **params):
        super().__init__(**params)
        self.file_select = self.file_select or FileSelector()
        self.viewer = pn.widgets.CodeEditor.from_param(
            self.param.file_contents,
            min_height=500,
            sizing_mode="stretch_both",
            readonly=True,
            theme="monokai",
        )
        self.configure_file_selector()
        self._layout = pn.Column(
            self.file_select,
            pn.WidgetBox(
                self.options(),
                pn.widgets.Select.from_param(self.viewer.param.theme, margin=(5, 25)),
            ),
            pn.layout.GridBox(
                pn.indicators.String.from_param(self.file_select.param.file_path, font_size="18px", name=""),
                pn.widgets.StaticText(
                    value="press control + F to open search window",
                    align=("end", "center"),
                ),
                ncols=2,
                sizing_mode="stretch_width",
            ),
            self.viewer,
            sizing_mode="stretch_both",
        )

    @param.depends("uit_client", watch=True)
    def configure_file_selector(self):
        if self.uit_client and self.uit_client.connected:
            file_browser = self.file_browser_class(uit_client=self.uit_client)
            self.file_select = FileSelector(file_browser=file_browser)
            self._layout[0] = self.file_select
            self.file_select.toggle()
            self.file_select.param.watch(self.get_file_contents, "file_path")

            self.viewer.filename = self.file_select.param.file_path

    @staticmethod
    def make_wrap(string, wrap_len):
        lines = string.splitlines(keepends=True)
        wrapped_lines = list()
        for line in lines:
            while len(line) > wrap_len:
                wrapped_lines.append(line[:wrap_len] + "\n")
                line = line[wrap_len:]
            wrapped_lines.append(line)
        return "".join(wrapped_lines)

    @param.depends("update_btn", watch=True)
    def get_file_contents(self, event=None):
        if self.uit_client and self.uit_client.connected:
            file_contents = ""
            try:
                if self.cmd == "grep":
                    case = "-i" if self.grep_ignore_case else ""
                    options = f"{case} -C {self.grep_context} {self.grep_pattern}"
                else:
                    options = f"-n {self.n}"
                file_contents = self.uit_client.call(f"{self.cmd} {options} {self.file_select.file_path}")
                if self.line_wrap:
                    file_contents = self.make_wrap(file_contents, self.wrap_length)
            except Exception as e:
                logger.debug(e)

            self.file_contents = file_contents

    @param.depends("line_wrap", watch=True)
    def toggle_wrap_length_visibility(self):
        self.param.wrap_length.precedence *= -1

    @param.depends("cmd", watch=True)
    def update_options(self):
        multiplier = 1 if self.cmd == "grep" else -1
        for p_name in ("grep_pattern", "grep_context", "grep_ignore_case"):
            p = self.param[p_name]
            p.precedence = abs(p.precedence) * multiplier

        self.param.n.precedence = abs(self.param.n.precedence) * -1 * multiplier

    def options(self):
        return pn.Param(
            self,
            parameters=[
                "cmd",
                "n",
                "grep_pattern",
                "grep_context",
                "grep_ignore_case",
                "line_wrap",
                "wrap_length",
                "update_btn",
            ],
            widgets={
                "cmd": dict(widget_type=pn.widgets.Select, width=100, align="end"),
                "n": dict(widget_type=pn.widgets.Spinner, width=100, align="end"),
                "grep_pattern": dict(widget_type=pn.widgets.TextInput, width=100, align="end"),
                "grep_context": dict(widget_type=pn.widgets.Spinner, width=100, align="end"),
                "grep_ignore_case": dict(widget_type=pn.widgets.Checkbox, width=100, align="end"),
                "line_wrap": dict(widget_type=pn.widgets.Checkbox, width=100, align="end"),
                "wrap_length": dict(widget_type=pn.widgets.Spinner, width=100),
                "update_btn": dict(
                    widget_type=pn.widgets.Button,
                    button_type="primary",
                    width=100,
                    align="end",
                ),
            },
            default_layout=pn.Row,
            show_name=False,
        )

    def panel(self):
        return self._layout

    def __panel__(self):
        return self._layout


class AsyncFileViewer(FileViewer):
    uit_client = param.ClassSelector(class_=AsyncClient)
    file_browser_class = AsyncHpcFileBrowser

    @param.depends("update_btn", watch=True)
    async def get_file_contents(self, event=None):
        if self.uit_client.connected:
            self.viewer.loading = True
            file_contents = ""
            try:
                if self.cmd == "grep":
                    case = "-i" if self.grep_ignore_case else ""
                    options = f"{case} -C {self.grep_context} {self.grep_pattern}"
                else:
                    options = f"-n {self.n}"
                file_contents = await self.uit_client.call(f"{self.cmd} {options} {self.file_select.file_path}")
                if self.line_wrap:
                    file_contents = self.make_wrap(file_contents, self.wrap_length)
            except Exception as e:
                logger.debug(e)

            self.file_contents = file_contents
            self.viewer.loading = False
