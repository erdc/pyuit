import re
import logging
from functools import partial
from itertools import zip_longest

from bokeh.models import NumberFormatter
import param
import panel as pn

from .file_browser import HpcFileBrowser, create_file_browser, get_js_loading_code, FileSelector
from .utils import HpcBase, HpcConfigurable
from ..uit import QUEUES
from ..pbs_script import NODE_TYPES, factors, PbsScript
from ..job import PbsJob


logger = logging.getLogger(__name__)


class PbsScriptInputs(HpcBase):
    hpc_subproject = param.Selector(
        default=None,
        label="HPC Subproject",
        precedence=3,
        doc="The resource allocation code that will be used when submitting this job.",
    )
    subproject_usage = param.DataFrame(precedence=3.1, doc="Usage details about your available subproject allocations.")
    node_type = param.Selector(
        default="", objects=[], label="Node Type", precedence=5, doc="Type of node on which this job will be run."
    )
    nodes = param.Integer(
        default=1,
        bounds=(1, 1000),
        precedence=5.1,
        doc=(
            "Number of nodes to request for the job.\n\n"
            "**Note:** for array jobs, the number of nodes requested are available to each sub job."
        ),
    )
    processes_per_node = param.Selector(
        default=1,
        objects=[],
        label="Processes per Node",
        precedence=5.2,
        doc="Number of processes per node to request for the job.",
    )
    wall_time = param.String(
        default="01:00:00",
        label="Wall Time (HH:MM:SS)",
        precedence=6,
        doc=(
            "Maximum allowable time for the job to run.\n\n"
            "**Note:** for array jobs, the entire amount of wall time requested is available to each sub job."
        ),
    )
    wall_time_alert = pn.pane.Alert(visible=False)
    node_alert = pn.pane.Alert(visible=False)
    queue = param.Selector(
        default=QUEUES[0], objects=QUEUES, precedence=7, doc="Scheduling queue to which the job will be submitted."
    )
    max_wall_time = param.String(default="Not Found", label="Max Wall Time", precedence=7.1)
    max_nodes = param.String(default="Not Found", label="Max Processes", precedence=7.2)
    submit_script_filename = param.String(default="run.pbs", precedence=8)
    notification_email = param.String(
        label="Notification E-mail(s)",
        precedence=9,
        doc="E-mail address to receive notification(s) when the job starts and/or ends.",
    )
    notify_start = param.Boolean(default=True, label="when job begins", precedence=9.1)
    notify_end = param.Boolean(default=True, label="when job ends", precedence=9.2)

    SHOW_USAGE_TABLE_MAX_WIDTH = 960
    DEFAULT_PROCESSES_PER_JOB = 500
    wall_time_maxes = None
    node_maxes = None

    def __init__(self, **params):
        super().__init__(**params)
        self.workdir = FileSelector(
            title="Base Directory",
            show_browser=False,
            help_text=(
                "Base directory that the job's working directory path will be created in.\n\n"
                "**Note:** by default the job's working directory is: "
                f"`<BASE_DIRECTORY>/{PbsJob.DEFAULT_JOB_LABEL}/<JOB_NAME>.<TIMESTAMP>/`"
            ),
        )

    @param.depends("uit_client", watch=True)
    def set_file_browser(self):
        self.workdir.file_browser = create_file_browser(self.uit_client, patterns=[])

    @staticmethod
    def get_default(value, objects, default=None):
        """Verify that value exists in the objects list, otherwise return a default or the first item in the list"""
        if value in objects:
            return value
        if default in objects:
            return default
        return objects[0]

    @param.depends("uit_client", watch=True)
    async def update_hpc_connection_dependent_defaults(self):
        if not self.uit_client.connected:
            return

        queues_stats = await self.await_if_async(self.uit_client.get_raw_queue_stats())

        self.subproject_usage = await self.await_if_async(self.uit_client.show_usage(as_df=True))
        subprojects = self.subproject_usage["Subproject"].to_list()
        self.param.hpc_subproject.objects = subprojects
        self.hpc_subproject = self.get_default(self.hpc_subproject, subprojects)
        self.workdir.file_path = self.uit_client.WORKDIR.as_posix()
        self.param.node_type.objects = list(NODE_TYPES[self.uit_client.system].keys())
        self.node_type = self.get_default(self.node_type, self.param.node_type.objects, default="compute")
        self.param.queue.objects = await self.await_if_async(self.uit_client.get_queues())
        self.queue = self.get_default(self.queue, self.param.queue.objects)
        self.node_maxes = await self.await_if_async(
            self.uit_client.get_node_maxes(self.param.queue.objects, queues_stats)
        )
        self.max_nodes = self.node_maxes[self.queue]
        self.wall_time_maxes = await self.await_if_async(
            self.uit_client.get_wall_time_maxes(self.param.queue.objects, queues_stats)
        )
        self.max_wall_time = self.wall_time_maxes[self.queue]
        self.nodes = round(self.DEFAULT_PROCESSES_PER_JOB / self.processes_per_node)

    @param.depends("queue", watch=True)
    def update_queue_depended_bounds(self):
        if self.queue == "debug":
            self.wall_time = "00:10:00"

    def set_wall_time_alert(self, visible, alert_type="warning", message=""):
        self.wall_time_alert.alert_type = alert_type
        self.wall_time_alert.object = message
        self.wall_time_alert.visible = visible

    def set_node_alert(self, visible, alert_type="warning", message=""):
        self.node_alert.alert_type = alert_type
        self.node_alert.object = message
        self.node_alert.visible = visible

    @param.depends("queue", watch=True)
    def update_max_wall_time_info(self):
        if self.wall_time_maxes is not None:
            self.max_wall_time = self.wall_time_maxes[self.queue]

    @param.depends("queue", "wall_time", watch=True)
    def validate_wall_time(self):
        wall_time_pattern = r"[0-9]+\:[0-9]{2}\:[0-9]{2}"
        if re.fullmatch(wall_time_pattern, self.wall_time) is None:
            self.set_wall_time_alert(
                True,
                alert_type="danger",
                message="Wall time value is not formatted correctly",
            )
        elif re.fullmatch(wall_time_pattern, self.max_wall_time) is None:
            self.set_wall_time_alert(False)
        elif int(self.wall_time.replace(":", "")) > int(self.max_wall_time.replace(":", "")):
            self.set_wall_time_alert(
                True,
                alert_type="warning",
                message="Wall time is greater than maximum for queue",
            )
        else:
            self.set_wall_time_alert(False)

    @param.depends("queue", "node_type", watch=True)
    def update_node_bounds(self):
        if self.node_maxes is not None:
            self.max_nodes = self.node_maxes[self.queue]

    @param.depends("queue", "nodes", "processes_per_node", watch=True)
    def validate_node_cores(self):
        if self.max_nodes == "Not Found":
            self.set_node_alert(False)
        else:
            total_process = self.nodes * self.processes_per_node
            if total_process > int(self.max_nodes):
                self.set_node_alert(
                    True,
                    alert_type="warning",
                    message="Number of processes is greater than maximum for queue",
                )
            else:
                self.set_node_alert(False)

    @param.depends("node_type", watch=True)
    def update_processes_per_node(self):
        self.param.processes_per_node.objects = factors(NODE_TYPES[self.uit_client.system][self.node_type])
        self.processes_per_node = self.param.processes_per_node.objects[-1]

    def add_email_directives(self, pbs_script):
        if self.notify_start or self.notify_end:
            options = ""
            if self.notify_start:
                options += "b"
            if self.notify_end:
                options += "e"
            pbs_script.set_directive("-m", options)
        if self.notification_email:
            pbs_script.set_directive("-M", self.notification_email)

    def pbs_options_view(self):
        return pn.Column(
            pn.Column(
                pn.Card(
                    pn.widgets.Tabulator.from_param(
                        self.param.subproject_usage,
                        width=self.SHOW_USAGE_TABLE_MAX_WIDTH,
                        show_index=False,
                        disabled=True,
                        formatters={
                            "Hours Allocated": NumberFormatter(format="0,0"),
                            "Hours Used": NumberFormatter(format="0,0"),
                            "Hours Remaining": NumberFormatter(format="0,0"),
                            "Background Hours Used": NumberFormatter(format="0,0"),
                        },
                    ),
                    title="Subproject Usage Summary",
                    collapsed=True,
                    margin=(10, 0),
                    width=self.SHOW_USAGE_TABLE_MAX_WIDTH + 20,
                )
            ),
            pn.Column(
                self.param.hpc_subproject,
                self.workdir,
                self.param.node_type,
                pn.widgets.Spinner.from_param(self.param.nodes),
                self.param.processes_per_node,
                self.param.wall_time,
                self.wall_time_alert,
                self.node_alert,
                self.param.queue,
                pn.Row(
                    pn.widgets.StaticText.from_param(self.param.max_wall_time),
                    pn.widgets.StaticText.from_param(self.param.max_nodes),
                ),
            ),
            pn.layout.WidgetBox(
                pn.widgets.TextInput.from_param(self.param.notification_email, placeholder="john.doe@example.com"),
                pn.pane.HTML('<label class"bk">Send e-mail notifications:</label>'),
                pn.widgets.Checkbox.from_param(self.param.notify_start, width=150),
                pn.widgets.Checkbox.from_param(self.param.notify_end, width=150),
            ),
            name="PBS Options",
        )


class PbsScriptAdvancedInputs(HpcConfigurable):
    env_names = param.List()
    env_values = param.List()
    env_browsers = param.List()
    env_delete_buttons = param.List()
    file_browser = param.ClassSelector(class_=HpcFileBrowser)
    file_browser_wb = param.ClassSelector(class_=pn.layout.WidgetBox)
    apply_file_browser = param.Action(label="Apply")
    close_file_browser = param.Action(lambda self: self.show_file_browser(False), label="Close")
    append_path = param.Boolean(label="Append to Path")

    def __init__(self, **params):
        super().__init__(**params)
        self.environment_variables_card = pn.Card(
            title="Environment Variables",
            sizing_mode="stretch_width",
            margin=(10, 0),
        )
        self.update_environment_variables_col()
        self.file_browser_wb = pn.WidgetBox(
            self.file_browser,
            pn.Row(
                pn.widgets.Checkbox.from_param(self.param.append_path, width=100),
                pn.widgets.Button.from_param(
                    self.param.apply_file_browser,
                    button_type="success",
                    width=100,
                ),
                pn.widgets.Button.from_param(
                    self.param.close_file_browser,
                    button_type="primary",
                    width=100,
                ),
                align="end",
            ),
            sizing_mode="stretch_width",
        )

    @param.depends("uit_client", watch=True)
    def configure_file_browser(self):
        self.file_browser = create_file_browser(self.uit_client)
        if self.file_browser_wb:
            self.file_browser_wb[0] = self.file_browser

    def show_file_browser(self, show):
        self.environment_variables_card[-1] = self.file_browser_wb if show else None
        if not show:
            for btn in self.env_browsers:
                btn.loading = False

    def update_environ(self, event):
        _, widget_type, i = event.obj.css_classes[0].split("_")
        i = int(i)
        key = self.env_names[i].value
        if widget_type == "key":
            if i > -1:
                self.environment_variables[event.new] = self.environment_variables[event.old]
                del self.environment_variables[event.old]
            else:
                self.environment_variables[event.new] = None
        elif widget_type == "val":
            self.environment_variables[key] = event.new
        elif widget_type == "del":
            del self.environment_variables[key]

        self.param.trigger("environment_variables")

    def env_var_widget(self, val, tag, **kwargs):
        widget = pn.widgets.TextInput(value=val, css_classes=[tag], **kwargs)
        widget.param.watch(self.update_environ, ["value"], onlychanged=True)
        return widget

    def env_file_browser_widget(self, tag, **kwargs):
        widget = pn.widgets.Button(name="📂", css_classes=[tag], width=40, align="end", **kwargs)
        widget.on_click(self.toggle_file_browser)
        return widget

    def env_delete_btn(self, tag):
        btn = pn.widgets.Button(name="X", css_classes=[tag], width=35, align="end", button_type="danger")
        btn.on_click(self.update_environ)
        btn.js_on_click(args={"btn": btn}, code=get_js_loading_code("btn"))
        return btn

    def toggle_file_browser(self, event):
        button = event.obj
        button.loading = True
        _, is_key, i = button.css_classes[0].split("_")
        self.apply_file_browser = partial(self.update_file_path, index=int(i))
        self.show_file_browser(True)

    def update_file_path(self, _, index):
        if self.append_path:
            self.env_values[index].value += f":{self.file_browser.value[0]}"
        else:
            self.env_values[index].value = self.file_browser.value[0]

    @param.depends("environment_variables", watch=True)
    def update_environment_variables_col(self):
        self.environment_variables.pop("", None)  # Clear blank key if there is one
        self.env_names = list()
        self.env_values = list()
        self.env_browsers = list()
        self.env_delete_buttons = list()

        for i, (k, v) in enumerate(self.environment_variables.items()):
            name_widget = self.env_var_widget(val=k, tag=f"env_key_{i}")
            val_widget = self.env_var_widget(val=str(v), tag=f"env_val_{i}")
            browser_widget = self.env_file_browser_widget(tag=f"env_browser_{i}")
            delete_btn = self.env_delete_btn(tag=f"env_del_{i}")
            self.env_names.append(name_widget)
            self.env_values.append(val_widget)
            self.env_browsers.append(browser_widget)
            self.env_delete_buttons.append(delete_btn)

        new_key_wg = self.env_var_widget(val=None, tag="env_key_-1", placeholder="NEW_ENV_VAR")
        new_val_wg = self.env_var_widget(val=None, tag="env_val_-1", disabled=True)
        new_key_wg.jscallback(
            args={"val": new_val_wg},
            value=get_js_loading_code("val"),
        )
        self.env_names.append(new_key_wg)
        self.env_values.append(new_val_wg)

        self.env_names[0].name = "Name"
        self.env_values[0].name = "Value"

        self.environment_variables_card[:] = [
            pn.Row(k, v, b, d, sizing_mode="stretch_width")
            for k, v, b, d in zip_longest(
                self.env_names,
                self.env_values,
                self.env_browsers,
                self.env_delete_buttons,
            )
        ]
        self.environment_variables_card.append(None)

    def advanced_options_view(self):
        return pn.Column(
            self.environment_variables_card,
            pn.Card(
                "<h3>Modules to Load</h3>",
                pn.widgets.CrossSelector.from_param(self.param.modules_to_load, width=700),
                "<h3>Modules to Unload</h3>",
                pn.widgets.CrossSelector.from_param(self.param.modules_to_unload, width=700),
                title="Modules",
                sizing_mode="stretch_width",
                collapsed=True,
                margin=(10, 0),
            ),
            name="Environment",
        )


class HpcSubmit(PbsScriptInputs, PbsScriptAdvancedInputs):
    submit_btn = param.Action(
        lambda self: self.param.trigger("submit_btn"),
        label="Submit",
        constant=True,
        precedence=10,
    )
    validate_btn = param.Action(
        lambda self: self.param.trigger("validate_btn"),
        label="Validate",
        constant=True,
        precedence=10,
    )
    cancel_btn = param.Action(lambda self: self.param.trigger("cancel_btn"), label="Cancel", precedence=10)
    previous_btn = param.Action(lambda self: self._previous(), label="Previous", precedence=10)
    disable_validation = param.Boolean(label="Override Validation")
    validated = param.Boolean()
    job_name = param.String(label="Job Name (Required, cannot contain spaces or tabs)")
    error_messages = param.ClassSelector(class_=pn.Column, default=pn.Column(sizing_mode="stretch_width"))
    _job = param.ClassSelector(class_=PbsJob, default=None)
    ready = param.Boolean(default=False, precedence=-1)
    next_stage = param.Selector()
    pipeline_obj = param.ClassSelector(class_=pn.pipeline.Pipeline)

    # TODO should this be here?  (see line 324)
    user_workspace = None

    def _previous(self):
        prev_stage = self.pipeline_obj._stages[self.pipeline_obj._prev_stage][0]
        prev_stage.reset()
        self.pipeline_obj.param.trigger("previous")
        self.pipeline_obj._block = False

    async def pre_validate(self):
        pass

    async def pre_submit(self):
        pass

    @param.output(jobs=list)
    async def submit(self):
        if self.job:
            if not self.job.job_id:
                self.job.script = self.pbs_script  # update script to ensure it reflects any UI updates
                await self.await_if_async(self.job.submit())
            return [self.job]

    @param.depends("submit_btn", watch=True)
    async def _submit(self):
        if not self.param.submit_btn.constant:
            self.param.submit_btn.constant = True
            await self.await_if_async(self.pre_submit())
            result = await self.submit()
            self.ready = bool(result)
            return result

    def validate(self):
        return True

    @param.depends("validate_btn", watch=True)
    async def _validate(self):
        if not self.param.validate_btn.constant:
            self.param.validate_btn.constant = True
            await self.await_if_async(self.pre_validate())
            is_valid = await self.await_if_async(self.validate())
            self.validated = is_valid
            if is_valid:
                param.depends(
                    self.param.job_name,
                    self.param.environment_variables,
                    self.param.modules_to_load,
                    self.param.modules_to_unload,
                    watch=True,
                )(self.un_validate)
            else:
                self.param.validate_btn.constant = False
                self.param.trigger("validated")

    async def un_validate(self, *events):
        if self.validated:
            await self.await_if_async(self.cancel())
            self.validated = False
            self.is_submitable()

    @param.depends("cancel_btn", watch=True)
    async def triggered_cancel(self):
        await self.await_if_async(self.cancel())

    def cancel(self):
        pass

    @property
    def pbs_script(self):
        pbs_script = PbsScript(
            name=self.job_name,
            project_id=self.hpc_subproject,
            num_nodes=self.nodes,
            queue=self.queue,
            processes_per_node=self.processes_per_node,
            node_type=self.node_type,
            max_time=self.wall_time,
            system=self.uit_client.system,
        )

        self.add_email_directives(pbs_script=pbs_script)

        # remove "(default)" from any modules when adding to pbs script
        for module in self.modules_to_load:
            pbs_script.load_module(module.replace("(default)", ""))
        for module in self.modules_to_unload:
            pbs_script.unload_module(module.replace("(default)", ""))

        pbs_script._environment_variables = self.environment_variables
        pbs_script.execution_block = self.execution_block

        return pbs_script

    @property
    def job(self):

        if self._job is None:
            self._job = PbsJob(
                script=self.pbs_script,
                client=self.uit_client,
                workspace=self.user_workspace,
                base_dir=self.workdir.file_path,
            )
        return self._job

    @property
    def execution_block(self):
        return ""

    @param.depends("job_name", watch=True)
    def is_submitable(self, error_messages: list = None):
        self.error_messages[:] = error_messages or []
        if not self.job_name:
            self.error_messages.append(
                pn.pane.Alert(
                    "* You must first enter a Job Name above before you can proceed.",
                    alert_type="danger",
                )
            )
        elif re.match(r"^[^*&%\\/\s]*$", self.job_name) is None:  # noqa: W605
            self.error_messages.append(
                pn.pane.Alert(
                    "* Job Name cannot contain spaces or any of the following characters: * & % \\ /",
                    alert_type="danger",
                )
            )
        errors_exist = len(self.error_messages) > 0
        self.param.submit_btn.constant = self.param.validate_btn.constant = self.param.disable_validation.constant = (
            errors_exist
        )
        self.param.trigger("disable_validation")  # get buttons to reload

    @param.depends("disable_validation", "validated")
    def action_button(self):
        if self.disable_validation or self.validated:
            button = "submit_btn"
            button_type = "success"
        else:
            button = "validate_btn"
            button_type = "primary"

        action_btn = pn.widgets.Button.from_param(self.param[button], button_type=button_type, width=200)
        cancel_btn = pn.widgets.Button.from_param(self.param.cancel_btn, button_type="danger", width=200)

        code = f"{get_js_loading_code('btn')} other_btn.disabled=true;"  # noqa
        action_btn.js_on_click(args={"btn": action_btn, "other_btn": cancel_btn}, code=code)
        cancel_btn.js_on_click(
            args={"other_btn": action_btn, "btn": cancel_btn},
            code=code,
        )
        return pn.Row(action_btn, cancel_btn)

    def submit_view(self):
        self.is_submitable()
        return pn.Column(
            self.view(),
            self.action_button,
            self.error_messages,
            name="Submit",
            sizing_mode="stretch_both",
        )

    def view(self):
        # override to customize submit tab
        return pn.Param(self.param.job_name)

    def panel(self):
        return pn.Column(
            "# Submit Job",
            pn.widgets.Button.from_param(self.param.previous_btn, button_type="primary", width=100),
            pn.layout.Tabs(
                self.submit_view(),
                self.pbs_options_view(),
                self.advanced_options_view(),
                active=1,
                sizing_mode="stretch_both",
            ),
            sizing_mode="stretch_both",
        )
