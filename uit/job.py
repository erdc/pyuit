import re
from pathlib import Path

from .uit import Client
from .pbs_script import PbsScript


class PbsJob:

    def __init__(self, script, client=None, working_dir=None):
        self.script = script
        self.client = client or Client()
        self._working_dir = working_dir
        self._job_id = None
        self._status = None

    @property
    def name(self):
        return self.script.name

    @property
    def working_dir(self):
        return Path(self._working_dir)

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
        """Submit a PBS Script.

        Args:
            pbs_script(PbsScript or str): PbsScript instance or string containing PBS script.
            working_dir(str): Path to working dir on supercomputer in which to run pbs script.
            remote_name(str): Custom name for pbs script on supercomputer. Defaults to "run.pbs".
            local_temp_dir(str): Path to local temporary directory if unable to write to os temp dir.

        Returns:
            bool: True if job submitted successfully.
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
        log_contents = self.client.call(f'cat {self._get_log_file_path(log_type)}')

        if filename is not None:
            with Path(filename).open('w') as log:
                log.write(log_contents)

        return log_contents

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

    def __init__(self, script, **kwargs):
        assert script._array_indices is not None
        super().__init__(script, **kwargs)
        self._sub_jobs = None

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
