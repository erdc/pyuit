import os
import glob
import logging
from pathlib import Path, PurePosixPath
from functools import wraps

import param
import panel as pn
# import panel.models.ace  # noqa: F401

from uit.uit import Client

log = logging.getLogger(__name__)


class FileManager(param.Parameterized):
    directory = param.String(
        default=os.getcwd(),
        precedence=0.1
    )
    file_keyword = param.String(
        doc='Keyword for file name. Hidden from ui.',
        default='*',
        precedence=-1
    )

    def __init__(self, **params):
        super().__init__(**params)
        self.cross_selector = pn.widgets.CrossSelector(name='Files', value=[], options=[], width=900)
        self._update_files()

    @property
    def value(self):
        return self.cross_selector.value

    @param.depends('directory', watch=True)
    def _update_files(self):
        self.cross_selector.options = glob.glob(os.path.join(self.directory, '*' + self.file_keyword + '*'))

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
    uit_client = param.ClassSelector(
        Client,
        precedence=-1
    )

    @param.depends('uit_client', watch=True)
    def _initialize_directory(self):
        if self.uit_client:
            self.directory = str(self.uit_client.WORKDIR)
            self._update_files()

    @param.depends('directory', watch=True)
    def _update_files(self):
        if self.uit_client:
            # get the ls from the client
            ls_df = self.uit_client.list_dir(
                path=self.directory,
                parse=True,
                as_df=True)

            # catch for errors returned as dict
            if type(ls_df) is dict:
                raise RuntimeError(f"""
                Request for directory list returned the error: {ls_df['error']}

                Directory requested: {self.directory}

                """)

            # convert dataframe to file list
            self.file_list = ls_df['path'].to_list()

            # update cross selector widget
            self.cross_selector.options = self.file_list

    def panel(self):
        return pn.Column(self.param.directory, self.cross_selector, width=700)


class FileTransfer(param.Parameterized):
    uit_client = param.ClassSelector(
        Client,
        precedence=-1
    )

    from_location = param.ObjectSelector(
        default='onyx',
        objects=['jim', 'onyx', 'local'],
        precedence=0.21
    )
    from_directory = param.String(
        precedence=0.22
    )
    to_location = param.ObjectSelector(
        default='onyx',
        objects=['jim', 'onyx', 'local'],
        precedence=0.31
    )
    to_directory = param.String(
        precedence=0.32
    )
    file_manager = param.ClassSelector(
        class_=FileManager,
        precedence=0.4
    )
    transfer_button = param.Action(lambda self: self.param.trigger('transfer_button'), label='Transfer', precedence=1.0)

    def __init__(self, uit_client, **params):

        super().__init__(**params)
        self.uit_client = uit_client or Client()
        self.file_manager = FileManagerHPC(uit_client=self.uit_client)

        # adjust to/from based on uit_client
        self.param.from_location.objects = [self.uit_client.system, 'local']
        self.from_location = self.uit_client.system
        self.param.to_location.objects = [self.uit_client.system, 'local']
        self.to_location = self.uit_client.system

    @param.depends('transfer_button', watch=True)
    def transfer(self):
        if self.from_location == 'local':
            for local_file in self.file_manager.cross_selector.value:
                self.uit_client.put_file(local_file, self.to_directory)
        elif self.to_location == 'local':
            for remote_file in self.file_manager.cross_selector.value:
                log.info('transferring {}'.format(remote_file))
                self.uit_client.get_file(remote_file,
                                         local_path=os.path.join(self.to_directory, os.path.basename(remote_file)))

        else:
            log.warning('HPC to HPC transfers are not supported.')

    @param.depends('from_directory', watch=True)
    def _update_file_manager(self):
        """
        """
        self.file_manager.directory = self.from_directory

    def _from_location(self):
        return pn.Column(self.param.from_location, self.param.from_directory)

    @param.depends('from_location', watch=True)
    def _to_location(self):
        remote_dir = str(self.uit_client.WORKDIR)
        local_dir = os.getcwd()

        if self.from_location == 'local':
            # set from location and dir
            self.from_directory = local_dir

            # set to location and dir
            self.to_location = self.uit_client.system
            self.to_directory = remote_dir

            # set file manager to local manager
            self.file_manager = FileManager()
        else:
            # set to location and dir
            self.to_location = 'local'
            self.to_directory = local_dir
            self.from_directory = remote_dir

            # set file manager to hpc manager
            self.file_manager = FileManagerHPC(uit_client=self.uit_client)

        # set cross selector directory
        self.file_manager._update_files()

    @param.depends('from_directory', watch=True)
    def panel(self):
        from_box = pn.WidgetBox(
            pn.Column(
                self._from_location,
                pn.Column(self.file_manager.cross_selector)
            )
        )

        to_box = pn.WidgetBox(
            pn.Column(self.param.to_location, self.param.to_directory),
            width=900,
            width_policy='max'
        )

        return pn.Column(
            from_box,
            to_box,
            pn.panel(self.param.transfer_button)
        )


class FileBrowser(param.Parameterized):
    """
    """
    path = param.ClassSelector(Path, precedence=-1)
    path_text = param.String(label='', precedence=0.3)
    home = param.Action(lambda self: self.go_home(), label='🏠', precedence=0.1)
    up = param.Action(lambda self: self.move_up(), label='⬆️', precedence=0.2)
    refresh_control = param.Action(lambda self: self.refresh(), label='🔄', precedence=0.25)
    callback = param.Action(lambda x: None, label='Select', precedence=0.4)
    file_listing = param.ListSelector(default=[], label='', precedence=0.5)
    patterns = param.List(precedence=-1, default=['*'])
    show_hidden = param.Boolean(default=False, label='Show Hidden Files', precedence=0.35)
    spn = pn.widgets.indicators.LoadingSpinner(value=True, color='primary', aspect_ratio=1, width=50)
    show_loading = param.Boolean(default=False)


    def __init__(self, delayed_init=False, **params):
        self.delayed_init = delayed_init
        super().__init__(**params)
        self._initialize_path()

    def init(self):
        self.delayed_init = False
        self._initialize_path()

    def _initialize_path(self):
        if self.delayed_init:
            return

        if self.path_text:
            self.validate()

        if not self.path:
            self.go_home()
        else:
            self.make_options()

    def _new_path(self, path):
        return Path(path)

    @property
    def controls(self):
        return ['home', 'up', 'refresh_control']

    @property
    def control_styles(self):
        styles = {c: {'width': 25} for c in self.controls}

        styles.update(
            path_text={'width_policy': 'max'},
            callback={'width': 100, 'button_type': 'success'},
        )
        return styles


    @param.depends('show_loading', 'file_listing', 'path_text')
    def loading(self):
        if isinstance(self.file_listing, list):
            print(str(self.path_text))
            # print(type(self.file_listing[0]))

            if len(self.file_listing) > 0:
                # print(self.param.file_listing.objects)
                print(self.file_listing[0]) # selected
                if str(self.path_text).endswith(str(self.file_listing[0])):
                    self.show_loading = False
        if self.show_loading:
            return pn.Column(self.spn)

    def toggle_loading(self, event=None):
        self.show_loading = True

    @property
    def panel(self):
        select_btn = pn.Param(
            self.param.callback,
            widgets={'callback': {'width': 100, 'button_type': 'success'}}
        )[0]
        select_btn.on_click(self.toggle_loading)

        return pn.Column(
            pn.Row(
                pn.Param(
                    self,
                    parameters=self.controls + ['path_text'],
                    widgets=self.control_styles,
                    default_layout=pn.Row,
                    width_policy='max',
                    show_name=False,
                    margin=0,
                ),
            select_btn, self.loading,
            ),
            self.param.show_hidden,
            pn.Param(self.param.file_listing, widgets={'file_listing': {'height': 200}}, width_policy='max'),
            width_policy='max',
            margin=0,
        )

    # def update_browser_bar(self):
    #     wg = pn.Param(self.param.file_listing, widgets={'file_listing': {'height': 200}}, width_policy='max')
    #     wg.param.watch(self.show_spinner, 'value')

    # def show_spinner(self, event):
    #     browser_bar[2] = pn.widgets.indicators.LoadingSpinner(value=True, color='primary', aspect_ratio=1, width=0)
    #     self.param.trigger('path')
        
    @property
    def value(self):
        if self.file_listing:
            return [str(self.path / v) for v in self.file_listing]
        else:
            return [self.path.as_posix()]

    def go_home(self):
        self.path = Path.cwd()

    def move_up(self):
        self.path = self.path.parent

    @param.depends('file_listing', watch=True)
    def move_down(self):
        for filename in self.file_listing:
            fn = self.path / filename
            if fn.is_dir():
                self.path = fn
                self.make_options()
            if self.callback:
                self.callback(True)

    def refresh(self):
        self.file_listing = ['.']

    @param.depends('path_text', watch=True)
    def validate(self):
        """Check that inputted path is valid - set validator accordingly"""
        path = self._new_path(self.path_text)
        if path and path.is_dir():
            self.path = path
        elif path and path.is_file():
            self.path = path.parent
        else:
            log.warning(f'Invalid Directory: {path}')

    @param.depends('path', 'show_hidden', watch=True)
    def make_options(self):
        self.path_text = self.path.as_posix()
        selected = []
        try:
            selected = [p.name + '/' for p in self.path.glob('*') if p.is_dir()]
            for pattern in self.patterns:
                selected.extend([p.name for p in self.path.glob(pattern) if not p.is_dir()])
            if not self.show_hidden:
                selected = [p for p in selected if not str(p).startswith('.')]
        except Exception as e:
            log.exception(str(e))

        self.file_listing = []
        self.param.file_listing.objects = sorted(selected)


class HpcPath(Path, PurePosixPath):
    """PurePath subclass that can make some system calls on an HPC system.

    """

    def _init(self, template=None, is_dir=None, uit_client=None):
        super()._init(template=template)
        self._is_dir = is_dir
        self._ls = []
        self.uit_client = uit_client

    def __new__(cls, *args, is_dir=None, uit_client=None):
        self = cls._from_parts(args, init=False)
        self._init(is_dir=is_dir, uit_client=uit_client)
        return self

    def __truediv__(self, key):
        new_path = super().__truediv__(key)
        new_path.uit_client = self.uit_client
        return new_path

    def _ensure_connected(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            if self.uit_client and self.uit_client.connected:
                return method(self, *args, **kwargs)
            log.warning('Path has no uit client, or it is not connected!')

        return wrapper

    @property
    def ls(self):
        if not self._ls:
            self._get_metadata()
        return self._ls

    @_ensure_connected
    def _get_metadata(self):
        if not self.is_absolute():
            self._str = str(self.uit_client.HOME / self)
        ls = self.uit_client.list_dir(self.parent.as_posix())
        if 'dirs' not in ls:  # then ls is invalid
            raise ValueError(f'Invalid file path {self.parent.as_posix()}')
        self._is_dir = False
        self._is_file = False
        self._ls = None

        # compare names instead of full path to handle symbolic links
        if self.name in (d['name'] for d in ls['dirs']):
            self._is_dir = True
            self._ls = self.uit_client.list_dir(self.as_posix())
            if 'error' in self._ls:
                self.parse_list_dir()
        elif self.name in (f['name'] for f in ls['files']):
            self._is_file = True

    def parse_list_dir(self):
        TYPES = {'d': 'dir', '-': 'file', 'l': 'link'}
        base_path = self.as_posix()
        parsed_ls = {'path': base_path, 'dirs': [], 'files': [], 'links': []}
        ls = self.uit_client.call(f'ls -l {base_path}')
        for f in ls.splitlines()[1:]:
            parts = f.split()
            perms, _, owner, group, size, mon, day, time, filename = parts[:9]
            metadata = {
                'owner': owner,
                'path': f'{base_path}/{filename}',
                'size': int(size),
                'lastmodified': f'{mon} {day} {time}',
                'name': filename,
                'perms': perms,
                'type': TYPES[perms[0]],
                'group': group
            }
            if perms.startswith('l'):
                metadata['link'] = parts[-1]
            parsed_ls[metadata['type'] + 's'].append(metadata)
        self._ls = parsed_ls

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
            file_path = p['path'].rsplit('/')[0] + f'/{p["name"]}'
            file_list.append(HpcPath(file_path, is_dir=is_dir, uit_client=self.uit_client))
        return file_list

    def glob(self, pattern):
        result = list()
        result.extend(self._get_file_list(self.ls['dirs'], is_dir=True))
        result.extend(self._get_file_list(self.ls['files'], is_dir=False))
        return [r for r in result if r.match(pattern)]


class HpcFileBrowser(FileBrowser):
    path = param.ClassSelector(HpcPath)
    workdir = param.Action(lambda self: self.go_to_workdir(), label='⚙️', precedence=0.15)
    uit_client = param.ClassSelector(Client)

    def __init__(self, uit_client, **params):
        params['uit_client'] = uit_client
        super().__init__(**params)

    @property
    def controls(self):
        controls = super().controls
        controls.insert(1, 'workdir')
        return controls

    def _new_path(self, path):
        return HpcPath(path, uit_client=self.uit_client)

    @param.depends('uit_client', watch=True)
    def _initialize_path(self):
        super()._initialize_path()

    @HpcPath._ensure_connected
    def go_home(self):
        self.path = self._new_path(self.uit_client.HOME)

    @HpcPath._ensure_connected
    def go_to_workdir(self):
        self.path = self._new_path(self.uit_client.WORKDIR)



class SelectFile(param.Parameterized):
    file_path = param.String(default='')
    show_browser = param.Boolean(default=False)
    browse_toggle = param.Action(lambda self: self.toggle(), label='Browse')
    file_browser = param.ClassSelector(FileBrowser)
    title = param.String(default='File Path')
    help_text = param.String()

    def __init__(self, **params):
        super().__init__(**params)
        self.file_browser = self.file_browser or FileBrowser(delayed_init=True)

    @param.depends('file_browser', watch=True)
    def update_callback(self):
        self.file_browser.callback = self.update_file

    def update_file(self, new_selection):
        if new_selection:
            self.file_path = self.file_browser.value[0]

    def toggle(self):
        self.show_browser = not self.show_browser
        self.param.browse_toggle.label = 'Hide' if self.show_browser else 'Browse'

    @param.depends('show_browser')
    def file_browser_panel(self):
        if self.show_browser:
            self.file_browser.path_text = self.file_path
            self.file_browser.init()
            return self.file_browser.panel

    @property
    def panel(self):
        spn = pn.widgets.indicators.LoadingSpinner(value=True, color='primary', aspect_ratio=1, width=0)

        browse_toggle = pn.Param(
            self.param.browse_toggle,
            widgets={'browse_toggle': {'button_type': 'primary', 'width': 100, 'align': 'end'}}
        )[0]
        browse_toggle.js_on_click(args={'btn': browse_toggle, 'spn': spn}, code='btn.visible=true; spn.width=50;')

        filepath_row = pn.Row(
            pn.Param(
                self,
                parameters=['file_path'],
                widgets={
                    'file_path': {'width_policy': 'max', 'show_name': False},
                },
                default_layout=pn.Row,
                show_name=False,
                width_policy='max',
                margin=0,
            ),
            browse_toggle, spn
        )
        self.param.file_path.label = self.title

        return pn.Column(
            filepath_row,
            pn.pane.HTML(f'<span style="font-style: italic;">{self.help_text}</span>'),
            self.file_browser_panel,
            width_policy='max'
        )


class FileViewer(param.Parameterized):
    update_btn = param.Action(lambda self: self.get_file_contents(), label='Update', precedence=3)
    n = param.Integer(default=500, bounds=(0, 10_000), precedence=2)
    cmd = param.ObjectSelector(default='head', objects=['head', 'tail'], label='Command', precedence=1)
    file_select = param.ClassSelector(SelectFile, default=SelectFile())
    file_path = param.String()
    file_contents = param.String()
    uit_client = param.ClassSelector(Client)

    @param.depends('uit_client', watch=True)
    def configure_file_selector(self):
        if self.uit_client.connected:
            file_browser = HpcFileBrowser(uit_client=self.uit_client, delayed_init=True)
            self.file_select = SelectFile(file_browser=file_browser)
            self.file_select.toggle()
            self.configure_path()

            self.file_select.param.watch(self.get_file_contents, 'file_path')

    @param.depends('file_path', watch=True)
    def configure_path(self):
        self.file_path = self.file_path or str(self.uit_client.WORKDIR)
        self.file_select.file_browser.path_text = self.file_path
        self.file_select.update_file(True)

    def get_file_contents(self, event=None):
        if self.uit_client.connected:
            try:
                self.file_contents = self.uit_client.call(f'{self.cmd} -n {self.n} {self.file_select.file_path}')
            except Exception as e:
                log.debug(e)
                # self.file_contents = f'ERROR!: {e}'
                self.file_contents = ''
            self.param.trigger('update_btn')

    @param.depends('update_btn')
    def view(self):
        import panel.models.ace  # noqa: F401

        file_path = self.file_select.file_path
        return pn.Column(
            pn.widgets.TextInput(value=file_path, disabled=True),
            pn.widgets.Ace(value=self.file_contents, width_policy='max', height_policy='max', height=1000,
                           readonly=True, filename=file_path),
            width_policy='max',
            height_policy='max',
            max_height=1000,
        )

    def panel(self):
        return pn.Column(
            self.file_select.panel,
            pn.WidgetBox(
                pn.Param(
                    self,
                    parameters=['cmd', 'n', 'update_btn'],
                    widgets={
                        'cmd': {'width': 100},
                        'n': pn.widgets.Spinner(value=self.n, width=100, name=self.param.n.label),
                        'update_btn': {'button_type': 'primary', 'width': 100, 'align': 'end'}
                    },
                    default_layout=pn.Row,
                    show_name=False,
                ),
                width=400,
            ),
            self.view,
            width_policy='max',
            height_policy='max',
            max_height=1000,
        )
