import os
import glob
import logging
from pathlib import Path, PurePosixPath
from functools import wraps

import param
import panel as pn

from .uit import Client, HPC_SYSTEMS, QUEUES
from uit.pbs_script import NODE_TYPES, factors
from .job import PbsJob

log = logging.getLogger(__name__)


class HpcAuthenticate(param.Parameterized):
    uit_client = param.ClassSelector(Client)
    authenticated = param.Boolean(default=False)
    auth_code = param.String(default='', label='Code')

    def __init__(self, uit_client=None, web_based=True, **params):
        super().__init__(**params)
        self.web_based = web_based
        self.uit_client = uit_client or Client()
        self.update_authenticated(bool(self.uit_client.token))

    def update_authenticated(self, authenticated=False):
        self.authenticated = authenticated
        self.param.trigger('authenticated')

    @param.depends('auth_code', watch=True)
    def get_token(self):
        self.uit_client.get_token(auth_code=self.auth_code)

    @param.depends('authenticated')
    def view(self):
        if not self.authenticated:
            header = '<h1>Authenticate to HPC</h1> ' \
                     '<h2>Instructions:</h2> ' \
                     '<p>Login to UIT+ with your CAC and then click the "Approve" button to authorize ' \
                     'this application to use your HPC account on your behalf.</p>'

            auth_frame = self.uit_client.authenticate(inline=True, callback=self.update_authenticated)
            if self.web_based:
                return pn.Column(header, auth_frame)
            header += '<p>When the "Success" message is shown copy the code (the alpha-numeric sequence after ' \
                      '"code=") and paste it into the Code field at the bottom. Then click "Authenticate".</p>'
            return pn.Column(header, auth_frame, self.param.auth_code,
                             pn.widgets.Button(name='Authenticate', button_type='primary', width=200))

        return pn.pane.HTML("<h1>Successfully Authenticated!</h1><p>Click Next to continue</p>", width=400)

    def panel(self):
        return pn.panel(self.view)


class HpcConnection(param.Parameterized):
    uit_client = param.ClassSelector(Client)
    system = param.ObjectSelector(default=HPC_SYSTEMS[1], objects=HPC_SYSTEMS)
    login_node = param.ObjectSelector(default=None, objects=[None], label='Login Node')
    exclude_nodes = param.ListSelector(default=list(), objects=[], label='Exclude Nodes')
    connected = param.Boolean(default=False)
    connect_btn = param.Action(lambda self: self.connect(), label='Connect')
    disconnect_btn = param.Action(lambda self: self.disconnect(), label='Disconnect')
    connection_status = param.String(default='Not Connected', label='Status')

    def __init__(self, uit_client=None, **params):
        super().__init__(**params)
        self.uit_client = uit_client or Client()
        self.update_node_options()

    @param.depends('system', watch=True)
    def update_node_options(self):
        options = [f'{self.system}{i:02d}' for i in range(1, 8)]
        self.param.exclude_nodes.objects = options
        options = options.copy()
        options.insert(0, None)
        self.param.login_node.objects = options
        self.param.login_node.names = {'Random': None}

    @param.depends('login_node', watch=True)
    def update_exclude_nodes_visibility(self):
        self.param.exclude_nodes.precedence = 1 if self.login_node is None else -1

    @param.output(uit_client=Client)
    def next(self):
        if not self.connected:
            self.connect()
        return self.uit_client

    def connect(self):
        system = None if self.login_node is not None else self.system
        self.connection_status = self.uit_client.connect(
            system=system,
            login_node=self.login_node,
            exclude_login_nodes=self.exclude_nodes,
        )
        self.connected = self.uit_client.connected

    def disconnect(self):
        self.param.connect_btn.label = 'Connect'
        self.connection_status = 'Not Connected'
        self.connected = False

    @param.depends('connected')
    def view(self):
        connect_btn = pn.Param(
            self, parameters=['connect_btn'],
            widgets={'connect_btn': {'button_type': 'success', 'width': 100}},
            show_name=False
        )

        if not self.connected:
            system_pn = pn.Column(
                pn.panel(self, parameters=['system'], show_name=False),
                connect_btn,
                name='HPC System',
            )
            advanced_pn = pn.Column(
                pn.panel(
                    self,
                    parameters=['login_node', 'exclude_nodes'],
                    widgets={'exclude_nodes': pn.widgets.CrossSelector},
                    show_name=False,
                ),
                connect_btn,
                name='Advanced Options',
            )

            return pn.Column(
                '<h1>Connect to HPC System</h1>',
                pn.panel(pn.layout.Tabs(system_pn, advanced_pn)),
            )
        else:
            self.param.connect_btn.label = 'Re-Connect'
            btns = pn.Param(
                self,
                parameters=['connect_btn', 'disconnect_btn'],
                widgets={
                    'disconnect_btn': {'button_type': 'danger', 'width': 100},
                    'connect_btn': {'button_type': 'success', 'width': 100}
                },
                show_name=False,
                default_layout=pn.Row,
            )
            return pn.Column(btns, pn.panel(self, parameters=['connection_status'], show_name=False, width=400))

    def panel(self):
        return pn.panel(self.view)


class HPCSubmitScript(param.Parameterized):
    hpc_subproject = param.ObjectSelector(default=None, precedence=3)
    workdir = param.String(default='', precedence=4)
    node_type = param.ObjectSelector(default='', objects=[], precedence=5)
    nodes = param.Integer(default=1, bounds=(1, 100), precedence=5.1)
    processes_per_node = param.ObjectSelector(default=1, objects=[], precedence=5.2)
    wall_time = param.String(default='00:05:00', precedence=6)
    queue = param.ObjectSelector(default='debug', objects=QUEUES, precedence=7)
    submit_script_filename = param.String(default='run.pbs', precedence=8)

    def update_hpc_conneciton_dependent_defaults(self):
        if not self.uit_client.connected:
            return

        subprojects = [u['subproject'] for u in self.uit_client.show_usage()]
        self.param.hpc_subproject.objects = subprojects
        self.hpc_subproject = subprojects[0]
        self.workdir = self.uit_client.WORKDIR.as_posix()
        self.param.node_type.objects = list(NODE_TYPES[self.uit_client.system].keys())
        self.node_type = self.param.node_type.objects[0]
        self.param.queue.objects = self.uit_client.get_queues()

    @param.depends('node_type', watch=True)
    def update_processes_per_node(self):
        self.param.processes_per_node.objects = factors(NODE_TYPES[self.uit_client.system][self.node_type])
        self.processes_per_node = self.param.processes_per_node.objects[-1]


class PbsScriptStage(HPCSubmitScript):
    submit_btn = param.Action(lambda self: self.submit(), label='Submit', precedence=10)
    uit_client = param.ClassSelector(Client)

    def submit(self):
        pass

    def view(self):
        self.update_hpc_conneciton_dependent_defaults()
        hpc_submit = pn.panel(self, parameters=list(HPCSubmitScript.param), show_name=False, name='PBS Options')
        return hpc_submit

    def panel(self):
        return pn.Row(self.view)


class JobMonitor(param.Parameterized):
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
        default='topaz',
        objects=['topaz', 'onyx', 'local'],
        precedence=0.21
    )
    from_directory = param.String(
        precedence=0.22
    )
    to_location = param.ObjectSelector(
        default='topaz',
        objects=['topaz', 'onyx', 'local'],
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
    callback = param.Action(lambda x: None, label='Select', precedence=0.4)
    file_listing = param.ListSelector(default=[], label='', precedence=0.5)
    patterns = param.List(precedence=-1)
    show_hidden = param.Boolean(default=False, label='Show Hidden Files', precedence=0.35)

    def __init__(self, **params):
        super().__init__(**params)
        self._initialize_path()

    def _initialize_path(self):
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
        return ['home', 'up']

    @property
    def control_styles(self):
        styles = {c: {'width': 25} for c in self.controls}

        styles.update(
            path_text={'width_policy': 'max'},
            callback={'width': 100, 'button_type': 'success'},
        )
        return styles

    @property
    def panel(self):
        return pn.Column(
            pn.Param(
                self,
                parameters=self.controls + ['path_text', 'callback'],
                widgets=self.control_styles,
                default_layout=pn.Row,
                width_policy='max',
                show_name=False,
                margin=0,
            ),
            self.param.show_hidden,
            self.param.file_listing,
            width_policy='max',
            margin=0,
        )

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
            elif self.callback:
                self.callback(True)

    @param.depends('path_text', watch=True)
    def validate(self):
        """Check that inputted path is valid - set validator accordingly"""
        path = self._new_path(self.path_text)
        if path and path.is_dir():
            self.path = path
        else:
            log.warning("Invalid Directory")

    @param.depends('path', 'show_hidden', watch=True)
    def make_options(self):
        self.path_text = self.path.as_posix()
        selected = []
        try:
            selected = [p.name + '/' for p in self.path.glob('*') if p.is_dir()]
            for pattern in self.patterns:
                selected.extend([p.name for p in self.path.glob(pattern)])
            if not self.show_hidden:
                selected = [p for p in selected if not str(p).startswith('.')]
        except Exception as e:
            log.exception(str(e))

        self.file_listing = []
        self.param.file_listing.objects = selected


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
        self._ls = self.uit_client.list_dir(self.as_posix())
        self._is_dir = 'path' in self.ls

    def is_dir(self):
        if self._is_dir is None:
            self._get_metadata()
        return self._is_dir

    def glob(self, pattern):
        result = list()
        if 'dirs' in self.ls:
            result.extend([HpcPath(p['path'], is_dir=True, uit_client=self.uit_client) for p in self.ls['dirs']])
            result.extend([HpcPath(p['path'], is_dir=False, uit_client=self.uit_client) for p in self.ls['files']])
        return [r for r in result if r.match(pattern)]


class HpcFileBrowser(FileBrowser):
    path = param.ClassSelector(HpcPath)
    workdir = param.Action(lambda self: self.go_to_workdir(), label='⚙️', precedence=0.15)
    uit_client = param.ClassSelector(Client)

    def __init__(self, uit_client, **params):
        super().__init__(**params)
        self.uit_client = uit_client
        self._initialize_path()

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
        self.file_browser = self.file_browser or FileBrowser()

    @param.depends('file_browser', watch=True)
    def update_callback(self):
        self.file_browser.callback = self.update_file

    def update_file(self, done):
        if done:
            self.file_path = self.file_browser.value[0]

    def toggle(self):
        self.show_browser = not self.show_browser
        self.browse_toggle.label = 'Hide' if self.show_browser else 'Browse'

    @param.depends('show_browser')
    def file_browser_panel(self):
        if self.show_browser:
            return self.file_browser.panel

    def input_row(self):
        return pn.Param(
            self,
            parameters=['file_path', 'browse_toggle'],
            widgets={
                'file_path': {'width_policy': 'max', 'show_name': False},
                'browse_toggle': {'button_type': 'primary', 'width': 100, 'align': 'end'}
            },
            default_layout=pn.Row,
            show_name=False,
            width_policy='max',
            margin=0,
        )

    @property
    def panel(self):
        self.param.file_path.label = self.title
        return pn.Column(
            self.input_row,
            pn.pane.HTML(f'<span style="font-style: italic;">{self.help_text}</span>'),
            self.file_browser_panel,
            width_policy='max'
        )
