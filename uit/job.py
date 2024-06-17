import os
import re
from datetime import datetime
from pathlib import PurePosixPath, Path
import logging

import pandas as pd

from .uit import Client
from .pbs_script import PbsScript, NODE_ARGS
from .execution_block import EXECUTION_BLOCK_TEMPLATE


logger = logging.getLogger(__name__)


class PbsJob:

    def __init__(self, script, client=None, label='pyuit', workspace=None,
                 transfer_input_files=None, home_input_files=None, archive_input_files=None,
                 working_dir=None, description=None, metadata=None, post_processing_script=None):
        self.script = script
        self.post_processing_script = post_processing_script
        self.client = client or Client()
        workspace = workspace or Path.cwd()
        self.workspace = Path(workspace)
        self.transfer_input_files = transfer_input_files or list()
        self.home_input_files = home_input_files or list()
        self.archive_input_files = archive_input_files or list()
        self.description = description
        self.metadata = metadata or dict()
        self.label = label
        self.cleanup = False
        self._working_dir = working_dir
        self._job_id = None
        self._status = None
        self._qstat = None
        self._post_processing_job_id = None
        self._remote_workspace_id = None
        self._remote_workspace = None

    def __repr__(self):
        return f'<{self.__class__.__name__} name={self.name} id={self.job_id}>'

    def __str__(self):
        return '\n  '.join((
            f'{self.__class__.__name__}:',
            f'Name: {self.name}',
            f'ID: {self.job_id}',
            f'Status: {self.status}',
            f'Working Dir: {self.working_dir}',
            f'Script: {self.script.__repr__()}',
        ))

    @property
    def name(self):
        return self.script.name

    @property
    def working_dir(self):
        if self._working_dir is None:
            self._working_dir = self.client.WORKDIR / self.remote_workspace_suffix
        return self._working_dir

    @property
    def run_dir(self):
        return self.working_dir

    @property
    def remote_workspace_id(self):
        """Get the timestamp associated with this job to be used as a workspace id.

        Returns:
            str: Remote workspace ID
        """

        if not self._remote_workspace_id:
            self._remote_workspace_id = datetime.now().strftime('%Y-%m-%d.%H_%M_%S.%f')
        return self._remote_workspace_id

    @property
    def remote_workspace_suffix(self):
        """Get the job specific suffix.

        Made up of a combination of label, name, and remote workspace ID.

        Returns:
            str: Suffix
        """
        if not self._remote_workspace:
            self._remote_workspace = PurePosixPath(self.label, f'{self.name}.{self.remote_workspace_id}')
        return self._remote_workspace

    @property
    def pbs_submit_script_name(self):
        return f'{self.name}.{self.remote_workspace_id}.pbs'

    @property
    def job_id(self):
        return self._job_id

    @property
    def post_processing_job_id(self):
        return self._post_processing_job_id

    @property
    def job_number(self):
        return re.split('\.|\[', self.job_id)[0]  # noqa: W605

    @property
    def status(self):
        return self._status

    @property
    def qstat(self):
        return self._qstat

    def submit(self, remote_name=None, local_temp_dir=''):
        """Submit a Job to HPC queue.

        Args:
            remote_name(str): Custom name for pbs script on supercomputer. Defaults to "run.pbs".
            local_temp_dir(str): Path to local temporary directory if unable to write to os temp dir.

        Returns:
            str: id of submitted job.
        """
        if self.job_id is not None:
            # TODO: log a warning stating that the job has already been submitted
            return self.job_id
        assert self.client.system == self.script.system

        working_dir = self.working_dir.as_posix()
        try:
            self.client.call(f'mkdir -p {working_dir}')
        except RuntimeError as e:
            raise RuntimeError('Error setting up job directory on "{}": {}'.format(self.system, str(e)))

        self._transfer_files()
        self._render_execution_block()

        remote_name = remote_name or self.pbs_submit_script_name

        self._job_id = self.client.submit(self.script, working_dir=working_dir,
                                          remote_name=remote_name, local_temp_dir=local_temp_dir)

        if self.post_processing_script:
            assert self.client.system == self.post_processing_script.system
            self.post_processing_script.set_directive('-W', f'depend=afterany:{self.job_id}')
            self._post_processing_job_id = self.client.submit(
                self.post_processing_script,
                working_dir=working_dir,
                remote_name=f'{remote_name.rstrip(".pbs")}_post_processing.pbs',
                local_temp_dir=local_temp_dir,
            )

        if remote_name == self.pbs_submit_script_name:
            self.client.call(f'mv {self.pbs_submit_script_name} {self.name}.{self.job_number}.pbs',
                             working_dir=working_dir)

        return self.job_id

    def set_default_post_processing_script(self, execution_block):
        self.post_processing_script = PbsScript(
            name=f'{self.name}_post_processing', project_id=self.script.project_id,
            num_nodes=1, processes_per_node=1, max_time='1:00:00', system=self.script.system,
            queue='transfer', node_type='transfer', execution_block=execution_block,
        )

    def terminate(self):
        if self.job_id:
            return self._execute('qdel', additional_options=self.post_processing_job_id)
        else:
            # Avoid running "qdel None" which causes an error. There is nothing to terminate in PBS, so return True.
            # This is often the result of a job that created files on the HPC but failed to create the PBS job.
            return True

    def hold(self):
        return self._execute('qhold')

    def release(self):
        return self._execute('qrls')

    def _execute(self, cmd, additional_options=None):
        additional_options = additional_options or ''
        try:
            self.client.call(command=f'{cmd} {self.job_id} {additional_options}')
            return True
        except Exception as e:
            if cmd == 'qdel' and ('qdel: Job has finished' in str(e) or 'qdel: Unknown Job Id' in str(e)):
                # qdel exits with rc=35 and 'qdel: Job has finished' if it is run on a job that has already finished
                # qdel exits with rc=153 and 'qdel: Unknown Job Id' if the job is too old
                # This basically tests if the HPC is working well enough to send 'rm -rf' commands next
                # This should not fail to delete from jobs_table when PBS no longer remembers the job
                return True
            else:
                logger.exception(f"Error when running '{cmd}': {e}")
                return False

    def _transfer_files(self):
        # Transfer any files listed in transfer_input_files to working_dir on supercomputer
        for transfer_file in self.transfer_input_files:
            transfer_file = Path(transfer_file)
            remote_path = self.working_dir / transfer_file.name
            ret = self.client.put_file(local_path=transfer_file, remote_path=remote_path)

            if ret.get('success') == 'false':
                raise RuntimeError('Failed to transfer input files: {}'.format(ret['error']))

    def _render_execution_block(self):
        execution_block = EXECUTION_BLOCK_TEMPLATE.format(
            archive_input_files=self._render_archive_input_files(),
            home_input_files=self._render_home_input_files(),
            execution_block=self.script.execution_block,
        )
        self.script._execution_block = execution_block

    def _render_home_input_files(self):
        return '\n'.join([f'cp ${{HOME}}/{f} .' for f in self.home_input_files])

    def _render_archive_input_files(self):
        return '\n'.join([f'archive get - C ${{ARCHIVE_HOME}} {f}' for f in self.archive_input_files])

    def _schedule_cleanup(self):
        pass
        # if self.cleanup:
        #     # Render cleanup script
        #     cleanup_walltime = strfdelta(self.max_cleanup_time, '%H:%M:%S')
        #     context = {
        #         'execute_job_id': execute_job_id,
        #         'execute_job_num': execute_job_id.split('.', 1)[0],
        #         'job_work_dir': self.working_dir,
        #         'job_archive_dir': self.archive_dir,
        #         'job_home_dir': self.home_dir,
        #         'project_id': self.project_id,
        #         'cleanup_walltime': cleanup_walltime,
        #         'archive_output_files': self.archive_output_files,
        #         'home_output_files': self.home_output_files,
        #         'transfer_output_files': self.transfer_output_files,
        #     }
        #
        #     cleanup_template = os.path.join(resources_dir, 'clean_after_exec.sh')
        #     with open(cleanup_template, 'r') as f:
        #         text = f.read()
        #         template = Template(text)
        #         cleanup_script = template.render(context)
        #     self.extended_properties['cleanup_job_id'] = self.client.submit(cleanup_script, self.working_dir,
        #                                                                     f'cleanup.{execute_job_id}.pbs')

    def update_status(self):
        self.update_statuses([self])
        return self.status

    def _get_log_file_name(self, log_type):
        """
        Returns the file name for `log_type`
        Args:
            log_type (str): the type of log: "o" for stdout or "e" for stderr

        Returns: The name of the log file for this job.

        """
        return f'{self.name}.{log_type}{self.job_number}'

    def _get_local_log_file_path(self, log_type: str):
        """
        Gets the local path where the log file is saved.
        Args:
            log_type (str): the type of log: "o" for stdout or "e" for stderr

        Returns: The full path to the log file in the local workspace (as a Path object).

        """
        return self.workspace / self._get_log_file_name(log_type)

    def _get_remote_log_file_path(self, log_type: str):
        """
        Gets the remote path (on the HPC) where the log file is created.
        Args:
            log_type (str): the type of log: "o" for stdout or "e" for stderr

        Returns: The full path to the log file on the HPC system (as a PurePosixPath object).

        """
        return self.working_dir / self._get_log_file_name(log_type)

    def get_cached_file_contents(self, remote_path, local_path=None, bytes=None, start_from=0):
        """
        Gets the file contents from a locally cached file. If file is not cached locally it will first copy the file from the HPC system to the local workspace.
        Args:
            remote_path (str, PurePosixPath): location of remote file to get contents of. It is resolved using `self.resolve_path`.
            local_path (str, Path): location to cache `remote_path`. Default is `None`. If not provided then it will be derived from `remote_path`.
            bytes (int): number of bytes to read from file. If `None` then the whole file will be read. Defaults to `None`.
            start_from (int): number of bytes to skip before reading file. Default is `0`. If negative it will read from the end of the end of the file.

        Returns: The contents of the file.

        """
        remote_path = self.resolve_path(remote_path)
        local_path = local_path or self.workspace / remote_path.relative_to(self.working_dir)
        if not local_path.exists():
            local_path.parent.mkdir(parents=True, exist_ok=True)
            self.client.get_file(remote_path, local_path)
            if not local_path.exists():
                raise RuntimeError(f'Unable to retrieve {remote_path}')

        size = local_path.stat().st_size
        if start_from > size:
            logger.warning(
                f'Requested to read contents of {local_path} starting from {start_from},'
                f' but the file is only {size} bytes long.'
            )
        with local_path.open('rb') as f:
            if start_from < 0:
                start_from = max(-size, start_from)
                f.seek(start_from, os.SEEK_END)
            else:
                f.seek(start_from)
            log_contents = f.read(bytes).decode()
        return log_contents

    def _get_cached_log(self, log_type, bytes=None, start_from=0):
        """
        Gets the contents of a log file. Checks local workspace first, and if not present copies the file from the HPC system to the local workspace.
        Args:
            log_type (str): the type of log: "o" for stdout or "e" for stderr

        Returns: The contents of the log. If the file cannot be copied from the HPC then raises a RuntimeError

        """
        local_path = self._get_local_log_file_path(log_type)
        remote_path = self._get_remote_log_file_path(log_type)
        return self.get_cached_file_contents(remote_path, local_path, bytes=bytes, start_from=start_from)

    def _get_log(self, log_type, filename=None, bytes=None, start_from=0):
        """
        Gets the contents of a log file, and optionally saves the file to disk.
        Args:
            log_type (str): the type of log: "o" for stdout or "e" for stderr
            filename (str, Path): file location to write the file contents to.

        Returns: File contents as a string.

        """
        try:
            if self.status in ['F', 'X']:
                log_contents = self._get_cached_log(log_type, bytes=bytes, start_from=start_from)
            else:
                log_contents = self.client.call(f'qpeek {self.job_id}')
                if log_contents == 'Unknown Job ID\n':
                    log_contents = self._get_cached_log(log_type, bytes=bytes, start_from=start_from)
                else:
                    index = {'o': 0, 'e': 1}[log_type]
                    log_parts = log_contents.split(f'{self.job_id.split(".")[0]} STDERR')[index].split('\n', 1)
                    try:
                        log_contents = log_parts[1]
                    except IndexError:
                        log_contents = ''
        except Exception as e:
            log_contents = f'ERROR retrieving log: {str(e)}'
            logger.exception(e)

        if filename is not None:
            with Path(filename).open('w') as log:
                log.write(log_contents)

        return log_contents

    def resolve_path(self, path):
        """
        Resolves strings with variables relating to the job id.
        """
        path = PurePosixPath(path).as_posix()
        path = path.replace('$JOB_NAME', self.name)
        path = path.replace('$JOB_ID', self.job_id)
        path = path.replace('$JOB_NUMBER', self.job_number)
        if self.post_processing_job_id:
            path = path.replace('$POST_PROCESSING_JOB_NUMBER', self._clean_job_id(self.post_processing_job_id))
        path = PurePosixPath(path)
        if path.is_absolute():
            return path
        return self.working_dir / path

    def get_stdout_log(self, filename=None, bytes=None, start_from=0):
        return self._get_log('o', filename, bytes=bytes, start_from=start_from)

    def get_stderr_log(self, filename=None, bytes=None, start_from=0):
        return self._get_log('e', filename, bytes=bytes, start_from=start_from)

    def get_custom_log(self, log_path, num_lines=None, head=False, filename=None):
        log_path = self.resolve_path(log_path)
        cmd = 'head' if head else 'tail'
        if num_lines is None:
            cmd = 'cat'
        else:
            cmd += f' -n {num_lines}'
        try:
            log_contents = self.client.call(f'{cmd} {log_path}')
        except RuntimeError as e:
            log_contents = str(e)

        if filename is not None:
            with Path(filename).open('w') as log:
                log.write(log_contents)

        return log_contents

    @staticmethod
    def _clean_job_id(job_id):
        return job_id.split('.')[0]

    @classmethod
    def update_statuses(cls, jobs, as_df=False):
        client = jobs[0].client
        job_ids = [j.job_id for j in jobs]
        job_ids.extend([j.post_processing_job_id for j in jobs if j.post_processing_job_id])
        statuses = client.status(job_ids, as_df=as_df)
        status_dicts = statuses.to_dict(orient='records') if as_df else statuses
        status_dicts = {cls._clean_job_id(status['job_id']): status for status in status_dicts}
        updated_status_dicts = {}
        for job in jobs:
            clean_job_id = cls._clean_job_id(job.job_id)
            status = status_dicts[clean_job_id]
            job._status = status['status']

            if status['status'] in ('F',) and job.post_processing_job_id:
                pp_status = status_dicts[cls._clean_job_id(job.post_processing_job_id)]
                job._status = pp_status['status']
                if job._status == 'Q':  # don't set the status back to "Submitted" for the post-processing job
                    job._status = 'R'

            if (
                    status['elapsed_time'] == '--'
                    and status['status'] not in ('Q', 'H', 'B')
                    and not isinstance(job, PbsArrayJob)
            ):
                if job._qstat['elapsed_time'][-1] != '*':
                    job._qstat['elapsed_time'] += '*'
                job._qstat['status'] = status['status']
            else:
                job._qstat = status

            updated_status_dicts[clean_job_id] = job._qstat

        statuses = pd.DataFrame.from_dict(updated_status_dicts).T if as_df else updated_status_dicts
        return statuses

    @classmethod
    def instance(cls, script, job_id, working_dir, client=None, status=None):
        instance = cls(
            script=script,
            client=client,
            working_dir=working_dir
        )
        instance._job_id = job_id
        instance._status = status
        return instance


class PbsArrayJob(PbsJob):
    class PbsArraySubJob(PbsJob):
        def __init__(self, parent, job_index):
            super().__init__(parent.script, parent.client, parent.label, parent.workspace,
                             working_dir=parent.working_dir)
            self.parent = parent
            self._remote_workspace_id = self.parent._remote_workspace_id
            self._remote_workspace = self.parent._remote_workspace
            self._job_index = job_index
            self._job_id = None

        @property
        def job_index(self):
            return self._job_index

        @property
        def job_id(self):
            if self._job_id is None and self.parent.job_id is not None:
                self._job_id = self.parent.job_id.replace('[]', f'[{self.job_index}]')
            return self._job_id

        @property
        def run_dir_name(self):
            return f'run_{self.job_index}'

        @property
        def run_dir(self):
            return self.working_dir / self.run_dir_name

        def submit(self, **kwargs):
            raise AttributeError('ArraySubJobs cannot be submitted. Submit must be called on the parent.')

        def _get_log_file_name(self, log_type):
            file_path = super()._get_log_file_name(log_type)
            return file_path + f'.{self.job_index}'

        def resolve_path(self, path):
            """
            Resolves strings with variables relating to the job id.
            """
            pppath = PurePosixPath(path)
            path = pppath.as_posix()
            path = path.replace('$JOB_INDEX', str(self.job_index))
            path = path.replace('$RUN_DIR', self.run_dir_name)
            return super().resolve_path(path)

    def __init__(self, script, **kwargs):
        assert script._array_indices is not None
        super().__init__(script, **kwargs)
        self._sub_jobs = None

    def update_status(self):
        all_jobs = [self] + self.sub_jobs
        self.update_statuses(all_jobs)
        self._qstat = {j.job_id: j.qstat for j in all_jobs}
        return self.status

    def _get_log(self, log_type, filename=None):
        raise AttributeError('Cannot get the log on a PbsArrayJob. You must access logs on the sub-jobs.')

    # @property
    # def job_array_ids(self):
    #     job_array_indices = self.script.job_array_indices
    #     if job_array_indices is not None and self.job_id is not None:
    #         job_id_template = self.job_id.replace('[]', '[{}]')
    #         return [job_id_template.format(i) for i in job_array_indices]

    @property
    def sub_jobs(self):
        if self._sub_jobs is None:
            self._sub_jobs = [self.PbsArraySubJob(self, job_index) for job_index in self.script.job_array_indices]
        return self._sub_jobs


def get_active_jobs(uit_client):
    jobs = list()
    statuses = uit_client.status(with_historic=True)
    if statuses:
        statuses = uit_client.status(job_id=[j['job_id'] for j in statuses], full=True)

        for job_id, status in statuses.items():
            j = get_job_from_full_status(job_id, status, uit_client)
            if j is not None:
                jobs.append(j)
    return jobs


def get_job_from_full_status(job_id, status, uit_client):
    Job = PbsJob
    node_type = 'transfer'
    for ntype, node_arg in NODE_ARGS.items():
        nnodes = int(status.get(f'Resource_List.{node_arg}', 0))
        if nnodes > 0:
            node_type = ntype
            break
    script = PbsScript(
        name=status['Job_Name'],
        project_id=status['Account_Name'],
        num_nodes=status['Resource_List.nodect'],
        processes_per_node=int(status['Resource_List.ncpus']) / int(status['Resource_List.nodect']),
        max_time=status['Resource_List.walltime'],
        queue=status['queue'].split('_')[0],
        node_type=node_type,
        system=uit_client.system,
    )
    if status.get('array'):
        Job = PbsArrayJob
        script._array_indices = tuple(int(i) for i in status['array_indices_submitted'].split('-'))
    try:
        working_dir = PurePosixPath(status['Output_Path'].split(':')[1]).parent.relative_to(uit_client.WORKDIR)
    except:  # noqa: E722
        return
    label = working_dir.parent.as_posix()
    remote_workspace_id = working_dir.name.split('.', 1)[1]
    j = Job(script=script, client=uit_client, label=label)
    j._remote_workspace_id = remote_workspace_id
    j._job_id = job_id
    j._status = status['job_state']
    return j


def get_job_from_id(job_id, uit_client, with_historic=True):
    status = uit_client.status(job_id=job_id, with_historic=with_historic, full=True)
    for job_id, full_status in status.items():
        return get_job_from_full_status(job_id, full_status, uit_client)


def _process_l_directives(pbs_script):
    matches = re.findall(r'^#PBS -l (.*)', pbs_script, flags=re.MULTILINE)
    d = dict()
    for match in matches:
        if 'walltime' in match:
            d['walltime'] = match.split('=')[1]
        else:
            d.update({k: v for k, v in [i.split('=') for i in match.split(':')]})
    return d


def get_job_from_pbs_script(job_id, pbs_script, uit_client):
    script = PurePosixPath(pbs_script)
    working_dir = script.parent
    logger.debug(f'PBS script parent: {working_dir}')
    pbs_script = uit_client.call(f'cat {pbs_script}')
    matches = re.findall(r'^#PBS -(.*)', pbs_script, flags=re.MULTILINE)
    directives = {k: v for k, v in [(i.split() + [''])[:2] for i in matches]}
    directives['l'] = _process_l_directives(pbs_script)

    Job = PbsJob
    script = PbsScript(
        name=directives['N'],
        project_id=directives['A'],
        num_nodes=int(directives['l']['select']),
        processes_per_node=int(directives['l']['ncpus']),
        max_time=directives['l']['walltime'],
        queue=directives['q'],
        system=uit_client.system,
    )
    if 'J' in directives:
        Job = PbsArrayJob
        script._array_indices = tuple(int(i) for i in re.split('[-:]', directives['J']))
        if not job_id.endswith('[]'):
            job_id += '[]'
    j = Job(script=script, client=uit_client, working_dir=working_dir)
    j._remote_workspace_id = working_dir.name
    try:
        j._remote_workspace_id = '.'.join(working_dir.name.split('.')[-3:])
    except:  # noqa: E722
        pass
    try:
        j.label = working_dir.parent.relative_to(uit_client.WORKDIR)
    except:  # noqa: E722
        pass
    j._job_id = job_id
    j._status = 'F'
    return j
