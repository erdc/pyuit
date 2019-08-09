from pathlib import Path

from .uit import Client
from .pbs_script import PbsScript


class PbsJob:

    def __init__(self, script=None, client=None, working_dir=None):
        self.script = script or PbsScript()
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
        return self.job_id.split('.')[0]

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

    def _get_log(self, log_type, filename=None):
        stdout_log = self.working_dir / f'{self.name}.{log_type}{self.job_number}'
        out = self.client.call(f'cat {stdout_log}')

        if filename is not None:
            with Path(filename).open('w') as log:
                log.write(out)

        return out

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
            assert job.job_id == status['job_id']
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
