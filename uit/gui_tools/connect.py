import logging

import param
import panel as pn

from uit.uit import Client, HPC_SYSTEMS, UITError

log = logging.getLogger(f'pyuit.{__name__}')


class HpcAuthenticate(param.Parameterized):
    uit_client = param.ClassSelector(Client)
    authenticated = param.Boolean(default=False)
    auth_code = param.String(default='', label='Code')
    ready = param.Boolean(default=False, precedence=-1)
    _next_stage = param.Selector()
    inline = param.Boolean(default=True, precedence=-1)

    def __init__(self, uit_client=None, web_based=True, **params):
        super().__init__(**params)
        self.web_based = web_based
        self.uit_client = uit_client or Client()
        self.update_authenticated(bool(self.uit_client.token))

    def update_authenticated(self, authenticated=False):
        self.ready = self.authenticated = authenticated
        self.param.trigger('authenticated')

    @param.depends('auth_code', watch=True)
    def get_token(self):
        self.uit_client.get_token(auth_code=self.auth_code)

    @param.depends('authenticated')
    def view(self):
        if not self.authenticated:
            popout_instructions = '' if self.inline else '<p>A new browser tab will open with the UIT+ site.</p>'
            header = '<h1>Authenticate to HPC</h1> ' \
                     '<h2>Instructions:</h2> ' \
                     f'{popout_instructions}<p>Login to UIT+ with your CAC and then click the "Approve" button to authorize ' \
                     'this application to use your HPC account on your behalf.</p>'

            auth_frame = self.uit_client.authenticate(inline=self.inline, callback=self.update_authenticated)
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
    system = param.ObjectSelector(default=HPC_SYSTEMS[0], objects=HPC_SYSTEMS)
    login_node = param.ObjectSelector(default=None, objects=[None], label='Login Node')
    exclude_nodes = param.ListSelector(default=list(), objects=[], label='Exclude Nodes')
    connected = param.Boolean(default=False, allow_None=True)
    connect_btn = param.Action(lambda self: self.connect(), label='Connect')
    disconnect_btn = param.Action(lambda self: self.disconnect(), label='Disconnect')
    connection_status = param.String(default='Not Connected', label='Status')
    ready = param.Boolean(default=False, precedence=-1)
    _next_stage = param.Selector()
    next_stage = param.Selector()

    def __init__(self, uit_client=None, **params):
        super().__init__(**params)
        self.uit_client = uit_client or Client()
        self.update_node_options()
        self.advanced_pn = None
        self.system_pn = pn.Column(
            pn.panel(self, parameters=['system'], show_name=False),
            pn.Spacer(),
            name='HPC System',
        )

    @param.depends('system', watch=True)
    def update_node_options(self):
        if self.uit_client.login_nodes:
            options = self.uit_client.login_nodes[self.system]
            self.param.exclude_nodes.objects = options
            options = options.copy()
            options.insert(0, None)
            self.param.login_node.objects = options
            self.param.login_node.names = {'Random': None}

    @param.depends('login_node', watch=True)
    def update_exclude_nodes_visibility(self):
        self.param.exclude_nodes.precedence = 1 if self.login_node is None else -1
        if self.login_node is None:
            self.advanced_pn.extend([
                self.param.exclude_nodes.label,
                pn.Param(
                    self.param.exclude_nodes,
                    widgets={'exclude_nodes': pn.widgets.CrossSelector},
                ),
            ])
        else:
            self.advanced_pn[:] = self.advanced_pn[:1]

    @param.depends('_next_stage', watch=True)
    def update_next_stage(self):
        self.next_stage = self._next_stage

    def connect(self):
        system = None if self.login_node is not None else self.system
        try:
            self.connected = None
            retry = self.login_node is None
            self.connection_status = self.uit_client.connect(
                system=system,
                login_node=self.login_node,
                exclude_login_nodes=self.exclude_nodes,
                retry_on_failure=retry,
            )
        except UITError as e:
            log.exception(e)
            self.exclude_nodes.append(self.uit_client.login_node)
            self.disconnect()
            self.system_pn[-1] = pn.pane.Alert(f'{e}<br/>Try connecting again.'.format(alert_type='danger'),
                                               alert_type='danger', width=500)
        else:
            self.connected = self.uit_client.connected
            self.ready = self.connected

    def disconnect(self):
        self.param.connect_btn.label = 'Connect'
        self.connection_status = 'Not Connected'
        self.system_pn[-1] = pn.Spacer()
        self.login_node = None
        self.connected = False

    @param.depends('connected')
    def view(self):
        header = '# Connect to HPC System'
        spn = pn.widgets.indicators.LoadingSpinner(value=True, color='primary', aspect_ratio=1, width=0)
        connect_btn = pn.Param(
            self.param.connect_btn,
            widgets={
                'connect_btn': {
                    'button_type': 'success',
                    'width': 100,
                }
            },
        )[0]
        connect_btn.js_on_click(args={'btn': connect_btn, 'spn': spn}, code='btn.visible=false; spn.width=50;')

        if self.connected is None:
            content = spn
        elif self.connected is False:
            self.advanced_pn = pn.panel(
                self,
                parameters=['login_node', 'exclude_nodes'],
                widgets={'exclude_nodes': pn.widgets.CrossSelector},
                show_name=False,
                name='Advanced Options',
            )
            if self.login_node is None:
                self.advanced_pn.insert(1, self.param.exclude_nodes.label)
            content = pn.Column(pn.layout.Tabs(self.system_pn, self.advanced_pn), connect_btn, spn)
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
            return pn.Column(
                header,
                btns,
                pn.panel(self, parameters=['connection_status'], show_name=False, width=400),
            )

        return pn.Column(header, content, width=500)

    def panel(self):
        return pn.panel(self.view)
