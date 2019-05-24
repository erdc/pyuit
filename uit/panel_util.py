import time
import os
import posixpath

import param
import panel as pn

from .uit import Client, HPC_SYSTEMS
from .job import PbsJob


class HpcConnection(param.Parameterized):
    uit_client = param.ClassSelector(Client)
    system = param.ObjectSelector(default=HPC_SYSTEMS[0], objects=HPC_SYSTEMS)
    login_node = param.ObjectSelector(default=None, objects=[None], label='Login Node')
    exclude_nodes = param.ListSelector(default=list(), objects=[], label='Exclude Nodes')
    authenticated = param.Boolean(default=False)
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

    def update_authenticated(self, authenticated=False):
        self.authenticated = authenticated
        self.param.trigger('authenticated')

    @param.depends('authenticated', 'connected')
    def view(self):
        if not self.authenticated:
            return pn.Column(self.uit_client.authenticate(notebook=True, callback=self.update_authenticated))
        elif not self.connected:
            system_pn = pn.Pane(self, parameters=['system'], show_name=False, name='HPC System')
            advanced_pn = pn.Pane(
                self,
                parameters=['login_node', 'exclude_nodes'],
                widgets={'exclude_nodes': pn.widgets.CrossSelector},
                show_name=False,
                name='Advanced Options'
            )

            return pn.Column(
                pn.Pane(pn.layout.Tabs(system_pn, advanced_pn)),
                pn.Pane(self, parameters=['connect_btn'], show_name=False)
            )
        else:
            self.param.connect_btn.label = 'Re-Connect'
            btns = pn.Row(
                pn.panel(self.param.disconnect_btn, show_name=False, width=200),
                pn.panel(self.param.connect_btn, show_name=False, width=200),
            )
            return pn.Column(btns, pn.Pane(self, parameters=['connection_status'], show_name=False, width=400))

    def panel(self):
        return pn.panel(self.view)


class SimulationInput(param.Parameterized):
    files = param.MultiFileSelector(default=[''], path='./')


class Solve(SimulationInput):

    def __init__(self, uit_client=None, **params):
        super(Solve, self).__init__(**params)
        self.uit_client = uit_client or Client()
        self.jobID = None

    def update_hpc_conneciton_dependent_defaults(self):
        if not self.uit_client.connected:
            return

        subprojects = [u['subproject'] for u in self.uit_client.show_usage()]
        self.param.hpc_subproject.default = subprojects[0]
        self.param.hpc_subproject.objects = subprojects
        self.workdir = self.uit_client.WORKDIR.as_posix()

    def pre_authenticate(self):
        # THIS ASSUMES A UIT CLIENT HAS ALREADY BEEN SET UP
        # instantiate client
        self.uit_client = uit.Client()

        # authenticate to the uit server
        result = self.uit_client.authenticate(notebook=self.notebook)

        # return an iFrame for authentication if notebook
        if self.notebook:
            return result

    def launch(self):
        if self.solve_on == 'desktop':
            self.launch_local()
        else:
            if self.solve_from == 'desktop':
                self.launch_hpc()
            else:
                print('solve from is still under development')

    def launch_local(self):
        # from roamsAPI.adh.sw2d import adhModel
        # instantiate roams adhModel
        adh_model = model.adhModel()
        # set hpcCompute variable
        hpcCompute = False
        # get local architecture
        architecture = self.local_architecture
        # solve locally
        adh_model.solveAdHModel(self.workdir, self.adh_rootname, hpcCompute, architecture)

    def launch_hpc(self):
        self.pre_authenticate()
        # connect to hpc
        #     c.connect(system='topaz', exclude_login_nodes=['topaz03','topaz07'])
        self.uit_client.connect(login_node=self.hpc_login_node)

        # create submit script
        self.uit_client.create_submit_script(
            self.hpc_subproject, nodes=self.nodes, walltime=self.wall_time,
                               project_name=self.adh_rootname,
                               filename=self.submit_script_filename, email=None, queue=self.queue)

        # add submit file to the list
        self.sim_files.append(self.submit_script_filename)

        # create the directory if necessary
        self.create_dir(self.workdir)

        # transfer the files to hpc
        self.move_files()

        # submit the job
        script_path = posixpath.join(self.workdir, self.submit_script_filename)
        resp = self.uit_client.call('qsub ' + script_path, working_dir=self.workdir)

        try:
            self.jobID = int(resp.split('.')[0])
        except:
            raise SystemError('Job did not submit correctly.')

        # query the job
        qstat = self.uit_client.call('qstat -u ' + self.uit_client.username, working_dir=self.workdir)
        print(qstat)

        # wait for job to complete
        self.wait_for_job(str(self.jobID))

    def wait_for_job(self, jobID, interval=60):
        while True:
            qview_resp = self.uit_client.call('qview', working_dir=self.workdir)
            if jobID in qview_resp:
                print('running')
                time.sleep(interval)
            else:
                break

        print('Job Completed.')

    def define_files(self, files):
        # clear out old files
        self.sim_files.clear()
        # add files to the widget
        [self.sim_files.append(file) for file in files]

    def move_files(self):
        # if authentication hasn't been done yet
        if self.uit_client is None:
            self.pre_authenticate()

        # create the hpc directory
        self.create_dir(self.workdir)

        for source_path in self.sim_files:
            # split the directory/filename from source
            head, tail = os.path.split(source_path)
            # create the destination path
            dest_path = posixpath.join(self.workdir, tail)
            print('Uploading file {} to {}'.format(source_path, dest_path))
            # transfer the file
            self.uit_client.put_file(source_path, dest_path)

    def create_dir(self, directory):
        """does nothing if directory already exists"""
        # if authentication hasn't been done yet
        if self.uit_client is None:
            self.pre_authenticate()

        # check that the directory exists
        resp = self.uit_client.list_dir(directory)

        # if dir does not exist
        if 'success' in resp.keys() and resp['success'] == 'false':
            self.uit_client.call('mkdir ' + directory, working_dir=directory)
            print('Directory {} created'.format(directory))

    def view(self):
        self.update_hpc_conneciton_dependent_defaults()
        solver = self
        inputinfo = pn.Pane(solver, parameters=list(SimulationInput.param),
                            widgets={'files': pn.widgets.CrossSelector}, show_name=False, name='Input')
        hpc_submit = pn.Pane(solver, parameters=list(HPCSubmitScript.param), show_name=False, name='HPC Options')
        return pn.Column(pn.Pane(pn.layout.Tabs(inputinfo, hpc_submit, height=500)),
                         pn.Pane(self, parameters=['submit_btn'], show_name=False))


class HPCSubmitScript(param.Parameterized):
    hpc_subproject = param.ObjectSelector(default=None, precedence=3)
    workdir = param.String(default='', precedence=4)
    nodes = param.Integer(default=1, bounds=(0, 5), precedence=5)
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


class PbsScriptStage(HPCSubmitScript):
    submit_btn = param.Action(lambda self: self.submit(), label='Submit')
    uit_client = param.ClassSelector(Client)

    def submit(self):
        pass

    def view(self):
        self.update_hpc_conneciton_dependent_defaults()
        hpc_submit = pn.Pane(self, parameters=list(HPCSubmitScript.param), show_name=False, name='PBS Options')
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
        self.update_statuses()

    @param.output(finished_job_ids=list)
    def finished_jobs(self):
        return self.statuses[self.statuses['status'] == 'F']['job_id'].tolist()

    @param.depends('uit_client', watch=True)
    def update_statuses(self):
        if self.uit_client:
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
