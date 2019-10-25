import param
import panel as pn

from uit.uit import Client, HPC_SYSTEMS


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


class HpcConnect(param.Parameterized):
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
