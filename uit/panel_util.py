import os
import glob

import param
import panel as pn

from .uit import Client, HPC_SYSTEMS
from uit.pbs_script import NODE_TYPES, factors
from .job import PbsJob


class HpcConnection(param.Parameterized):
    uit_client = param.ClassSelector(Client)
    system = param.ObjectSelector(default=HPC_SYSTEMS[1], objects=HPC_SYSTEMS)
    login_node = param.ObjectSelector(default=None, objects=[None], label='Login Node')
    exclude_nodes = param.ListSelector(default=list(), objects=[], label='Exclude Nodes')
    authenticated = param.Boolean(default=False)
    connected = param.Boolean(default=False)
    connect_btn = param.Action(lambda self: self.connect(), label='Connect')
    disconnect_btn = param.Action(lambda self: self.disconnect(), label='Disconnect')
    connection_status = param.String(default='Not Connected', label='Status')
    auth_code = param.String(default='', label='Code')

    def __init__(self, uit_client=None, web_based=True, **params):
        super().__init__(**params)
        self.web_based = web_based
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

    @param.depends('auth_code', watch=True)
    def get_token(self):
        self.uit_client.get_token(auth_code=self.auth_code)

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

    def update_authenticated(self, authenticated=False):
        self.authenticated = authenticated
        self.param.trigger('authenticated')

    @param.depends('authenticated', 'connected')
    def view(self):
        if not self.authenticated:
            header = '<h1>Step 1 of 2: Authorize and Authenticate</h1> ' \
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
        elif not self.connected:
            system_pn = pn.panel(self, parameters=['system'], show_name=False, name='HPC System')
            advanced_pn = pn.panel(
                self,
                parameters=['login_node', 'exclude_nodes'],
                widgets={'exclude_nodes': pn.widgets.CrossSelector},
                show_name=False,
                name='Advanced Options'
            )

            return pn.Column(
                '<h1>Step 2 of 2: Connect</h1>',
                pn.panel(pn.layout.Tabs(system_pn, advanced_pn)),
                pn.panel(self, parameters=['connect_btn'], show_name=False)
            )
        else:
            self.param.connect_btn.label = 'Re-Connect'
            btns = pn.Row(
                pn.panel(self.param.disconnect_btn, show_name=False, width=200),
                pn.panel(self.param.connect_btn, show_name=False, width=200),
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
    queue = param.ObjectSelector(default='debug', objects=['standard', 'debug'], precedence=7)
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
        return pn.layout.Tabs(
            pn.panel(self.status_panel, name='Status'),
            pn.panel(self.logs_panel, name='Logs'),
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

    def __init__(self, **params):
        if 'uit_client' in params:
            self.directory = str(params['uit_client'].WORKDIR)

        super().__init__(**params)
        self._update_files()

    @param.depends('directory', watch=True)
    def _update_files(self):
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
                print('transferring {}'.format(remote_file))
                self.uit_client.get_file(remote_file,
                                         local_path=os.path.join(self.to_directory, os.path.basename(remote_file)))

        else:
            print('HPC to HPC transfers are not supported.')

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
