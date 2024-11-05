import logging

import param
import panel as pn

from .utils import make_bk_label

from ..uit import Client
from ..exceptions import UITError, MaxRetriesError

logger = logging.getLogger(__name__)


class HpcAuthenticate(param.Parameterized):
    uit_client = param.ClassSelector(Client)
    authenticated = param.Boolean(default=False)
    auth_code = param.String(default="", label="Code")
    ready = param.Boolean(default=False, precedence=-1)
    _next_stage = param.Selector()

    def __init__(self, uit_client=None, web_based=True, **params):
        super().__init__(**params)
        self.web_based = web_based
        self.uit_client = uit_client or Client()
        self.update_authenticated(bool(self.uit_client.token))

    def update_authenticated(self, authenticated=False):
        self.ready = self.authenticated = authenticated
        self.param.trigger("authenticated")

    @param.depends("auth_code", watch=True)
    def get_token(self):
        self.uit_client.get_token(auth_code=self.auth_code)

    @param.depends("authenticated")
    def view(self):
        if not self.authenticated:
            header = (
                "<h1>Authenticate to HPC</h1> "
                "<h2>Instructions:</h2> "
                "<p>A new browser tab will open with the UIT+ site.</p>"
                '<p>Login to UIT+ with your CAC and then click the "Approve" button to authorize '
                "this application to use your HPC account on your behalf.</p>"
            )

            auth_frame = self.uit_client.authenticate(
                callback=self.update_authenticated
            )
            if self.web_based:
                return pn.Column(header, auth_frame)
            header += (
                '<p>When the "Success" message is shown copy the code (the alpha-numeric sequence after '
                '"code=") and paste it into the Code field at the bottom. Then click "Authenticate".</p>'
            )
            return pn.Column(
                header,
                auth_frame,
                self.param.auth_code,
                pn.widgets.Button(
                    name="Authenticate", button_type="primary", width=200
                ),
            )

        return pn.pane.HTML(
            "<h1>Successfully Authenticated!</h1><p>Click Next to continue</p>",
            width=400,
        )

    def panel(self):
        return pn.panel(self.view)


class HpcConnect(param.Parameterized):
    uit_client = param.ClassSelector(Client)
    system = param.Selector()
    login_node = param.Selector(default=None, objects=[None], label="Login Node")
    exclude_nodes = param.ListSelector(
        default=list(), objects=[], label="Exclude Nodes"
    )
    connected = param.Boolean(default=False, allow_None=True)
    connect_btn = param.Action(lambda self: self.connect(), label="Connect")
    disconnect_btn = param.Action(lambda self: self.disconnect(), label="Disconnect")
    connection_status = param.String(default="Not Connected", label="Status")
    ready = param.Boolean(default=False, precedence=-1)
    _next_stage = param.Selector()
    next_stage = param.Selector()

    def __init__(self, **params):
        super().__init__(**params)
        self.advanced_pn = pn.Column(name="Advanced Options")
        self.system_pn = pn.Column(
            pn.panel(self, parameters=["system"], show_name=False),
            pn.Spacer(),
            name="HPC System",
        )

    @param.depends("uit_client", watch=True)
    def update_system_options(self):
        if self.uit_client is not None:
            self.param.system.objects = self.uit_client.systems
            self.system = self.uit_client.systems[0]

    @param.depends("system", watch=True)
    def update_node_options(self):
        if self.uit_client is not None:
            options = self.uit_client.login_nodes[self.system]
            self.param.exclude_nodes.objects = options
            options = options.copy()
            options.insert(0, None)
            self.param.login_node.objects = options
            self.param.login_node.names = {"Random": None}
            self.update_exclude_nodes_visibility()

    @param.depends("login_node", watch=True)
    def update_exclude_nodes_visibility(self):
        self.advanced_pn.clear()
        self.advanced_pn.append(
            pn.widgets.Select.from_param(self.param.login_node, width=300)
        )
        if self.login_node is None:
            self.advanced_pn.extend(
                [
                    make_bk_label(self.param.exclude_nodes.label),
                    pn.widgets.CrossSelector.from_param(self.param.exclude_nodes),
                ]
            )

    @param.depends("_next_stage", watch=True)
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
        except (UITError, MaxRetriesError) as e:
            logger.exception(e)
            self.exclude_nodes.append(self.uit_client.login_node)
            self.disconnect()
            self.system_pn[-1] = pn.pane.Alert(
                "ERROR: Unable to establish a connection with the selected HPC system. "
                "This type of network error is often intermittent. Try connecting again in a few minutes.",
                alert_type="danger",
                width=500,
            )
        else:
            self.connected = self.uit_client.connected
            self.ready = self.connected

    def disconnect(self):
        self.param.connect_btn.label = "Connect"
        self.connection_status = "Not Connected"
        self.system_pn[-1] = pn.Spacer()
        self.login_node = None
        self.connected = False

    @param.depends("connected")
    def view(self):
        header = "# Connect to HPC System"
        connect_btn = pn.widgets.Button.from_param(
            self.param.connect_btn, button_type="success", width=100
        )
        connect_btn.js_on_click(
            args={
                "btn": connect_btn,
            },
            code='btn.css_classes.push("pn-loading", "pn-arc"); btn.properties.css_classes.change.emit();',
        )

        if self.connected is None:
            content = None
        elif self.connected is False:
            content = pn.Column(
                pn.layout.Tabs(self.system_pn, self.advanced_pn), connect_btn
            )
        else:
            self.param.connect_btn.label = "Re-Connect"
            connect_btn = pn.widgets.Button.from_param(
                self.param.connect_btn, button_type="success", width=100
            )
            disconnect_btn = pn.widgets.Button.from_param(
                self.param.disconnect_btn, button_type="danger", width=100
            )
            return pn.Column(
                header,
                pn.Row(connect_btn, disconnect_btn),
                pn.panel(
                    self, parameters=["connection_status"], show_name=False, width=400
                ),
            )

        return pn.Column(header, content, width=500)

    def panel(self):
        return pn.panel(self.view)
