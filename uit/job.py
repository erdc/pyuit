import re
import os
from pathlib import PurePosixPath, Path

from .uit import Client
from .pbs_script import PbsScript


class PbsJob:

    def __init__(self, script, client=None, working_dir=None):
        self.script = script
        self.client = client or Client()
        self._working_dir = working_dir
        self._job_id = None
        self._status = None

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
        return PurePosixPath(self._working_dir)

    @property
    def job_id(self):
        return self._job_id

    @property
    def job_number(self):
        return re.split('\.|\[', self.job_id)[0]

    @property
    def status(self):
        return self._status

    def submit(self, working_dir=None, remote_name=None, local_temp_dir=''):
        """Submit a Job to HPC queue.

        Args:
            working_dir(str): Path to working dir on supercomputer in which to run pbs script.
            remote_name(str): Custom name for pbs script on supercomputer. Defaults to "run.pbs".
            local_temp_dir(str): Path to local temporary directory if unable to write to os temp dir.

        Returns:
            str: id of submitted job.
        """
        if self.job_id is not None:
            # TODO: log a warning stating that the job has already been submitted
            return self.job_id
        # TODO: check to make sure system on self.client is compatible with self.script

        self._working_dir = working_dir or self.working_dir or self.client.WORKDIR / self.name
        self.client.call(f'mkdir -p {self.working_dir.as_posix()}')

        remote_name = remote_name or f'{self.name}_run.pbs'

        self._job_id = self.client.submit(self.script, working_dir, remote_name, local_temp_dir)

        return self.job_id

    def update_status(self):
        status = self.client.status(self.job_id)[0]
        self._status = status['status']
        return self.status

    def _get_log_file_path(self, log_type):
        return self.working_dir / f'{self.name}.{log_type}{self.job_number}'

    def _get_log(self, log_type, filename=None):
        try:
            if self.status in ['F']:
                log_contents = self.client.call(f'cat {self._get_log_file_path(log_type)}')
            else:
                log_contents = self.client.call(f'qpeek {self.job_id}')
                if log_contents == 'Unknown Job ID\n':
                    log_contents = self.client.call(f'cat {self._get_log_file_path(log_type)}')
                else:
                    index = {'o': 0, 'e': 1}[log_type]
                    log_contents = log_contents.split('\n\n')[index].split('\n', 1)[1]
        except RuntimeError as e:
            log_contents = str(e)

        if filename is not None:
            with Path(filename).open('w') as log:
                log.write(log_contents)

        return log_contents

    def resolve_path(self, path):
        """
        Resolves strings with variables relating to the job id.
        """
        path = path.replace('$JOB_ID', self.job_id)
        path = path.replace('$JOB_NUMBER', self.job_number)
        path = PurePosixPath(path)
        if path.is_absolute():
            return path
        return self.working_dir / path

    def get_stdout_log(self, filename=None):
        return self._get_log('o', filename)

    def get_stderr_log(self, filename=None):
        return self._get_log('e', filename)

    @classmethod
    def update_statuses(cls, jobs, as_df=False):
        client = jobs[0].client
        job_ids = [j.job_id for j in jobs]
        statuses = client.status(job_ids, as_df=as_df)
        status_dicts = statuses.to_dict(orient='records') if as_df else statuses
        for job, status in zip(jobs, status_dicts):
            assert job.job_id.startswith(status['job_id'])  # job id can be cutoff in the status output
            job._status = status['status']
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
            super().__init__(parent.script, parent.client, parent.working_dir)
            self.parent = parent
            self._job_index = job_index
            self._job_id = self.parent.job_id.replace('[]', f'[{self.job_index}]')

        @property
        def job_index(self):
            return self._job_index

        def submit(self, **kwargs):
            raise AttributeError('ArraySubJobs cannot be submitted. Submit must be called on the parent.')

        def _get_log_file_path(self, log_type):
            file_path = super()._get_log_file_path(log_type).as_posix()
            return file_path + f'.{self.job_index}'

        def resolve_path(self, path):
            """
            Resolves strings with variables relating to the job id.
            """
            path = path.replace('$JOB_INDEX', str(self.job_index))
            return super().resolve_path(path)

    def __init__(self, script, **kwargs):
        assert script._array_indices is not None
        super().__init__(script, **kwargs)
        self._sub_jobs = None

    def update_status(self):
        status = self.client.status(self.job_id)[0]
        self._status = status['status']
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
        if self._sub_jobs is None and self.job_id is not None:
            self._sub_jobs = [self.PbsArraySubJob(self, job_index) for job_index in self.script.job_array_indices]
        return self._sub_jobs


def get_active_jobs(uit_client):
    statuses = uit_client.status(with_historic=True)
    statuses = uit_client.status(job_id=[j['job_id'] for j in statuses], full=True)
    jobs = list()
    for job_id, status in statuses.items():
        j = get_job_from_full_status(job_id, status, uit_client)
        jobs.append(j)
    return jobs


def get_job_from_full_status(job_id, status, uit_client):
    Job = PbsJob
    script = PbsScript(
        name=status['Job_Name'],
        project_id=status['Account_Name'],
        num_nodes=status['Resource_List.compute'],
        processes_per_node=int(status['Resource_List.ncpus']) / int(status['Resource_List.compute']),
        max_time=status['Resource_List.walltime'],
        system=uit_client.system,
    )
    if status.get('array'):
        Job = PbsArrayJob
        script._array_indices = tuple(int(i) for i in status['array_indices_submitted'].split('-'))
    working_dir = os.path.dirname(status['Output_Path'].split(':')[1])
    j = Job(script=script, client=uit_client, working_dir=working_dir)
    j._job_id = job_id
    j._status = status['job_state']
    return j


def get_job_from_id(job_id, uit_client, with_historic=True):
    status = uit_client.status(job_id=job_id, with_historic=with_historic, full=True)
    for job_id, full_status in status.items():
        return get_job_from_full_status(job_id, full_status, uit_client)


def _process_l_directives(pbs_script):
    matches = re.findall('#PBS -l (.*)', pbs_script)
    d = dict()
    for match in matches:
        if 'walltime' in match:
            d['walltime'] = match.split('=')[1]
        else:
            d.update({k: v for k, v in [i.split('=') for i in matches[0].split(':')]})

    return d


def get_job_from_pbs_script(job_id, pbs_script, uit_client):
    script = PurePosixPath(pbs_script)
    working_dir = script.parent
    pbs_script = uit_client.call(f'cat {pbs_script}')
    matches = re.findall('#PBS -(.*)', pbs_script)
    directives = {k: v for k, v in [i.split() for i in matches]}
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
    working_dir = working_dir
    j = Job(script=script, client=uit_client, working_dir=working_dir)
    j._job_id = job_id
    j._status = 'F'
    return j
