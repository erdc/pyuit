import asyncio.exceptions
import inspect
import json
import logging
import os
import ssl
import tempfile
import time
import uuid
from pathlib import PurePosixPath, Path
from urllib.parse import urljoin

import param
import aiohttp

# optional dependency
try:
    import pandas as pd

    has_pandas = True
except ImportError:
    has_pandas = False

from .uit import (
    Client,
    UIT_API_URL,
    encode_pure_posix_path,
    FG_CYAN,
    ALL_OFF,
    _auth_code,
)
from .util import robust, AsyncHpcEnv
from .pbs_script import PbsScript
from .exceptions import UITError, MaxRetriesError

logger = logging.getLogger(__name__)
_ensure_connected = Client._ensure_connected


class AsyncClient(Client):
    """Provides a python abstraction for interacting with the UIT API.

    Args:
        ca_file (str):
        config_file (str): Location of a config file containing, among other things, the Client ID and Secret Key.
        client_id (str): ID issued by UIT to authorize this client.
        client_secret (str): Secret key associated with the client ID.
        scope (str):
        session_id (str): 16-digit Hexidecimal string identifying the current session. Auto-generated from urandom if
            not provided.
        token (str): Token from current UIT authorization.
        port (int):
    """

    _async_init = param.Parameter()

    def __init__(
        self,
        ca_file=None,
        config_file=None,
        client_id=None,
        client_secret=None,
        session_id=None,
        scope="UIT",
        token=None,
        port=5000,
        async_init=False,
    ):
        super().__init__(
            ca_file,
            config_file,
            client_id,
            client_secret,
            session_id,
            scope,
            token,
            port,
            delay_token=True,
        )
        self.env = AsyncHpcEnv(self)
        self._session = None
        if async_init:
            self.param.trigger("_async_init")
        elif self.token is not None:
            super().get_userinfo()

    @property
    def session(self):
        if self._session is None:
            ssl_context = ssl.create_default_context(cafile=self.ca_file)
            conn = aiohttp.TCPConnector(ssl=ssl_context)
            self._session = aiohttp.ClientSession(connector=conn)
        return self._session

    async def close_session(self):
        if self._session is not None:
            # await asyncio.sleep(0.25)  # wait for connections to close
            await self._session.close()

    @param.depends("_async_init", watch=True)
    async def get_token_dependent_info(self):
        if self.token is not None:
            await self.get_userinfo()

    async def connect(
        self,
        system=None,
        login_node=None,
        exclude_login_nodes=(),
        retry_on_failure=None,
        num_retries=3,
    ):
        """Connect this client to the UIT servers.

        Args:
            system (str): Specific system name to connect to. Cannot be used with login_node arg.
            login_node (str): Specific node name to connect to. Cannot be used with system arg.
            exclude_login_nodes (list): Nodes to exclude when selecting a login node. Ignored if login_node is
                specified.
            retry_on_failure (bool):
                True will attempt to connect to different login nodes.
                False will only attempt one connection.
                Default of None will automatically pick False if login_node is set, otherwise it will pick True.
            num_retries (int): Number of connection attempts. Requires retry_on_failure=True
        """
        # get access token from file
        # populate userinfo and header info
        login_node, retry_on_failure = self.prepare_connect(system, login_node, exclude_login_nodes, retry_on_failure)
        try:
            # working_dir='.' ends up being the location for UIT+ scripts, not the user's home directory
            # await self.call(':', working_dir='.', timeout=35)
            # initialize property environment variables
            await self.env.get_environmental_variable("HOME")
            await self.env.get_environmental_variable("WORKDIR")
            await self.env.get_environmental_variable("WORKDIR2")
            await self.env.get_environmental_variable("CENTER")
        except UITError as e:
            self.connected = False
            msg = f"Error while connecting to node {login_node}: {e}"
            logger.info(msg)
            if retry_on_failure is False:
                raise UITError(msg)
            elif retry_on_failure is True and num_retries > 0:
                # Try a different login node
                logger.debug(f"Retrying connection {num_retries} more time(s) to this HPC {system}")
                num_retries -= 1
                exclude_login_nodes = list(exclude_login_nodes) + [login_node]
                return await self.connect(
                    system=system,
                    exclude_login_nodes=exclude_login_nodes,
                    retry_on_failure=retry_on_failure,
                    num_retries=num_retries,
                )
            else:
                raise MaxRetriesError(msg)
        else:
            msg = f"Connected successfully to {login_node} on {system}"
            logger.info(msg)
            return msg

    @property
    def auth_func(self):
        # needs to be the synchronous version for the authentication server
        return super().get_token

    async def get_token(self, auth_code=None):
        """Get token from the UIT server.

        Args:
            auth_code (str): The authentication code to use.
        """

        url = urljoin(UIT_API_URL, "token")

        self._auth_code = auth_code or _auth_code

        # check for auth_code
        if self._auth_code is None:
            raise RuntimeError(
                "You must first authenticate to the UIT server and get a auth code. Then set the auth_code"
            )

        # set up the data dictionary
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "state": self.session_id,
            "scope": self.scope,
            "code": self._auth_code,
        }

        token = await self.session.post(url, data=data)

        # check the response
        if token.status == 200:
            logger.info("Access Token request succeeded.")
        else:
            raise IOError("Token request failed.")

        self.token = (await token.json())["access_token"]
        self._do_callback(True)

    async def get_userinfo(self):
        """Get User Info from the UIT server."""
        # request user info from UIT site
        response = await self.session.get(urljoin(UIT_API_URL, "userinfo"), headers=self.headers)
        data = await response.json()
        if not data["success"]:
            raise UITError("Not Authenticated")
        self._userinfo = data.get("userinfo")
        self._user = self._userinfo.get("USERNAME")
        logger.info(f"get_userinfo user='{self._user}'")
        self._systems = sorted([sys.lower() for sys in self._userinfo["SYSTEMS"].keys()])
        self._login_nodes = {
            system: [
                node["HOSTNAME"].split(".")[0] for node in self._userinfo["SYSTEMS"][system.upper()]["LOGIN_NODES"]
            ]
            for system in self._systems
        }

        self._uit_urls = [
            [
                {node["HOSTNAME"].split(".")[0]: node["URLS"]["UIT"]}
                for node in self._userinfo["SYSTEMS"][system.upper()]["LOGIN_NODES"]
            ]
            for system in self._systems
        ]
        self._uit_urls = {k: v for _list in self._uit_urls for d in _list for k, v in d.items()}  # noqa: E741

    @_ensure_connected
    @robust()
    async def call(
        self,
        command,
        working_dir=None,
        full_response=False,
        raise_on_error=True,
        timeout=120,
    ):
        """Execute commands on the HPC via the exec endpoint.

        Args:
            command (str): The command to run.
            working_dir (str, optional, default=None): Working directory from which to run the command.
                If None, the users $HOME directory will be used.
            full_response (bool, optional, default=False): If True return the full JSON response from the UIT+ server.
            raise_on_error (bool, optional, default=True): If True then an exception is raised if the command fails.
            timeout (int, optional, default=120): Number of seconds to limit the duration of the post() call.

        Returns:
            str: stdout from the command.
        """
        # Need to do this manually to prevent recursive loop when resolving self.home
        working_dir = working_dir or self.HOME

        working_dir = self._resolve_path(working_dir)

        # construct the base options dictionary
        data = {"command": command, "workingdir": working_dir}
        data = {"options": json.dumps(data, default=encode_pure_posix_path)}
        logger.info(f"call command='{FG_CYAN}{command}{ALL_OFF}'    {working_dir=}")
        debug_start_time = time.perf_counter()
        try:
            r = await self.session.post(
                urljoin(self._uit_url, "exec"),
                headers=self.headers,
                data=data,
                timeout=timeout,
            )
        except asyncio.exceptions.TimeoutError:
            if raise_on_error:
                raise UITError("Request Timeout")
            else:
                return "ERROR! Request Timeout"
        logger.debug(await self._debug_uit(locals()))

        if r.status == 504:
            if raise_on_error:
                raise UITError("Gateway Timeout")
            else:
                return "ERROR! Gateway Timeout"

        try:
            resp = await r.json()
        except aiohttp.client_exceptions.ContentTypeError as e:
            logger.error(
                "JSON Parse Error '%s' - Status code: %s  Content: %s",
                str(e),
                r.status,
                await r.text(),
            )
            raise

        if full_response:
            return resp
        if resp.get("success") == "true":
            return resp.get("stdout") + resp.get("stderr")
        elif raise_on_error:
            raise UITError(resp.get("error", resp.get("stderr", resp)))
        else:
            return f"ERROR!\n{resp.get('stdout')=}\n{resp.get('stderr')=}"

    @_ensure_connected
    @robust()
    async def put_file(self, local_path, remote_path=None, timeout=30):
        """Put files on the HPC via the putfile endpoint.

        Args:
            local_path (str): Local file to upload.
            remote_path (str): Remote file to upload to. Do not use shell shortcuts like ~ or variables like $HOME.
            timeout(int): Number of seconds to limit the duration of the post() call,
                although ongoing data transfer will not trigger a timeout.

        Returns:
            str: API response as json
        """
        local_path = Path(local_path)
        assert local_path.is_file()
        filename = local_path.name
        remote_path = self._resolve_path(remote_path, self.HOME / filename)
        data = {"file": remote_path}
        data = {"options": json.dumps(data, default=encode_pure_posix_path)}
        with local_path.open(mode="rb") as f:
            files = {"file": f}
            files.update(data)
            logger.info(f"put_file {local_path=}    {remote_path=}")
            debug_start_time = time.perf_counter()
            try:
                # async with self.session.post(...) as r:
                r = await self.session.post(
                    urljoin(self._uit_url, "putfile"),
                    headers=self.headers,
                    data=files,
                    timeout=timeout,
                )
            except asyncio.exceptions.TimeoutError:
                raise UITError("Request Timeout")
        logger.debug(await self._debug_uit(locals()))

        try:
            return await r.json()
        except aiohttp.client_exceptions.ContentTypeError as e:
            # UIT should always return JSON, but other services may return an HTML error
            logger.error(
                "JSON Parse Error '%s' - Status code: %s  Content: %s",
                str(e),
                r.status,
                await r.text(),
            )
            raise UITError("Upload error") from e

    @_ensure_connected
    @robust()
    async def get_file(self, remote_path, local_path=None, timeout=30):
        """Get a file from the HPC via the getfile endpoint.

        Args:
            remote_path (str): Remote file to download.
            local_path (str): local file to download to.
            timeout(int): Number of seconds to limit the duration of the post() call,
                although ongoing data transfer will not trigger a timeout.

        Returns:
            Path: local_path
        """
        remote_path = PurePosixPath(remote_path)
        local_path = Path(local_path) if local_path else Path() / remote_path.name
        remote_path = self._resolve_path(remote_path)
        data = {"file": remote_path}
        data = {"options": json.dumps(data, default=encode_pure_posix_path)}
        debug_start_time = time.perf_counter()
        logger.info(f"get_file {remote_path=}    {local_path=}")
        try:
            r = await self.session.post(
                urljoin(self._uit_url, "getfile"),
                headers=self.headers,
                data=data,
                timeout=None,
            )
        except asyncio.exceptions.TimeoutError:
            raise UITError("Request Timeout")

        if r.status != 200:
            raise RuntimeError(
                "UIT returned a non-success status code ({}). The file '{}' may not exist, or you may "
                "not have permission to access it.".format(r.status, remote_path)
            )
        # async with aiofiles.open(local_path, 'wb') as f:
        #     await f.write(await r.read())
        with open(local_path, "wb") as f:
            async for chunk in r.content.iter_chunked(4096):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
            local_file_size = f.tell()  # tell() returns the file seek pointer which is at the end of the file
        logger.debug(await self._debug_uit(locals()))

        return local_path

    async def _get_dir_stats(self, local_dir, remote_dir, get_dir=False):
        local_dir = Path(local_dir)
        remote_dir = PurePosixPath(remote_dir)
        # get remote dir stats
        try:
            remote_dir_stats = await self.call(f"find {remote_dir.as_posix()} -type f -printf '%Ts %s %P\n'")
            # Remote mtime is always an integer, so this ignores decimals for local and remote mtime
            # That avoids re-transferring files with microsecond timestamp differences
            remote_files = {
                stats[-1]: {"mtime": int(stats[0]), "size": int(stats[1])}
                for stats in [line.split() for line in remote_dir_stats.splitlines()]
            }
        except Exception as e:
            logger.debug(e)
            remote_files = {}
            if get_dir:
                return {}, {}, {}, {}

        # get local dir stats
        if get_dir:
            local_dir.mkdir(parents=True, exist_ok=True)
        local_files = {p.relative_to(local_dir).as_posix(): p.stat() for p in local_dir.glob("**/*") if p.is_file()}

        # compare
        not_local = {}
        not_remote = local_files
        for name, remote_stats in remote_files.items():
            # get local stats for same file name if it exists
            local_stats = local_files.get(name)

            # compare file size and mtime
            if not (
                local_stats
                and local_stats.st_size == remote_stats["size"]
                and local_stats.st_mtime == remote_stats["mtime"]
            ):
                not_local[name] = remote_stats
            else:
                not_remote.pop(name)

        return remote_files, local_files, not_remote, not_local

    @_ensure_connected
    @robust()
    async def put_dir(self, local_dir, remote_dir):
        _, local_files, not_remote, __ = await self._get_dir_stats(local_dir, remote_dir)

        async def put_file_with_stats(file_name):
            remote_file_path = PurePosixPath(remote_dir) / file_name
            local_file_path = Path(local_dir) / file_name
            await self.put_file(local_file_path, remote_file_path)
            mtime = int(local_files[file_name].st_mtime)
            await self.call(f"touch -d @{mtime} {remote_file_path}")

        remote_dirs_to_create = set()
        for file_name in not_remote:
            remote_dirs_to_create.add(Path(file_name).parent)

        if remote_dirs_to_create:
            logger.info(f"Creating {len(remote_dirs_to_create)} directories before file uploads")
            for this_remote_dir in remote_dirs_to_create:
                await self.call(f"mkdir -p {PurePosixPath(remote_dir) / this_remote_dir}")

        # transfer files that didn't match those on the hpc
        logger.info(f"Uploading {len(not_remote)} files")
        for file_name in not_remote:
            await put_file_with_stats(file_name)

    @_ensure_connected
    @robust()
    async def get_dir(self, remote_dir, local_dir):
        remote_files, _, __, not_local = await self._get_dir_stats(local_dir, remote_dir, get_dir=True)

        async def get_file_with_stats(file_name):
            remote_file_path = PurePosixPath(remote_dir) / file_name
            local_file_path = Path(local_dir) / file_name
            local_file_path.parent.mkdir(parents=True, exist_ok=True)
            await self.get_file(remote_file_path, local_file_path)
            mtime = remote_files[file_name]["mtime"]
            os.utime(local_file_path, (mtime, mtime))

        # transfer files that didn't match those on the hpc
        logger.info(f"Downloading {len(not_local)} files")
        for file_name in not_local:
            await get_file_with_stats(file_name)

    @_ensure_connected
    @robust()
    async def list_dir(self, path=None, parse=True, as_df=False, timeout=30):
        """Get a detailed directory listing from the HPC via the listdirectory endpoint.

        Args:
            path (str): Directory to list.
            parse (bool): False returns the output of 'ls -la', and True parses that into a JSON dictionary.
            as_df (bool): Return a pandas DataFrame.
            timeout(int): Number of seconds to limit the duration of the post() call.

        Returns:
            str: The API response as JSON
        """
        path = self._resolve_path(path, self.HOME)

        if not parse:
            return self.call(f"ls -la {path}")

        data = {"directory": path}
        data = {"options": json.dumps(data, default=encode_pure_posix_path)}
        logger.info(f"list_dir {path=}")
        debug_start_time = time.perf_counter()
        try:
            r = await self.session.post(
                urljoin(self._uit_url, "listdirectory"),
                headers=self.headers,
                data=data,
                timeout=timeout,
            )
        except asyncio.exceptions.TimeoutError:
            raise UITError("Request Timeout")
        logger.debug(await self._debug_uit(locals()))

        result = await r.json()

        if as_df and "path" in result:
            ls = result["dirs"]
            ls.extend(result["files"])
            columns = (
                "perms",
                "type",
                "owner",
                "group",
                "size",
                "lastmodified",
                "path",
                "name",
            )
            return self._as_df(ls, columns)
        return await r.json()

    @_ensure_connected
    @robust()
    async def show_usage(self, parse=True, as_df=False):
        """Get output from the `show_usage` command, which shows the subproject IDs

        Args:
            parse(bool, optional, default=True): return results parsed into a list of dicts rather than as a raw string.

        Returns:
            str: The API response
        """
        # 'module reload' is a workaround for users with a default shell of /bin/csh on Warhawk.
        # UIT+ runs commands in a bash script, and that combination drops part of the PATH for show_usage.
        result = await self.call("module reload >/dev/null 2>&1; show_usage")
        if not parse:
            return result

        return self._parse_hpc_output(result, as_df)

    @_ensure_connected
    @robust()
    async def status(
        self,
        job_id=None,
        username=None,
        full=False,
        with_historic=False,
        parse=True,
        as_df=False,
    ):
        username = username if username is not None else self.username

        cmd = "qstat"

        if full:
            cmd += " -f"
        elif username:
            cmd += f" -u {username}"

        if job_id:
            if isinstance(job_id, (tuple, list)):
                job_id = " ".join([j.split(".")[0] for j in job_id])
            cmd += f" -x {job_id}"
            result = await self.call(cmd)
            return self._process_status_result(result, parse=parse, full=full, as_df=as_df)
        else:
            # If no jobs are specified then
            result = await self.call(cmd)
            result1 = self._process_status_result(result, parse=parse, full=full, as_df=as_df)
            if not with_historic:
                return result1
            else:
                cmd += " -x"
                result = await self.call(cmd)
                result2 = self._process_status_result(result, parse=parse, full=full, as_df=as_df)

                if not parse:
                    return result1, result2
                elif as_df:
                    return pd.concat((result1, result2))
                else:
                    result1.extend(result2)
                    return result1

    @_ensure_connected
    async def submit(self, pbs_script, working_dir=None, remote_name="run.pbs", local_temp_dir=None):
        """Submit a PBS Script.

        Args:
            pbs_script(PbsScript or str): PbsScript instance or string containing PBS script.
            working_dir(str): Path to working dir on supercomputer in which to run pbs script.
            remote_name(str): Custom name for pbs script on supercomputer. Defaults to "run.pbs".
            local_temp_dir(str): Path to local temporary directory if unable to write to os temp dir.

        Returns:
            bool: True if job submitted successfully.
        """
        working_dir = PurePosixPath(self._resolve_path(working_dir, self.WORKDIR))

        local_temp_dir = local_temp_dir or tempfile.gettempdir()

        pbs_script_path = os.path.join(local_temp_dir, str(uuid.uuid4()))

        # Write out PbsScript tempfile
        if isinstance(pbs_script, PbsScript):
            pbs_script.write(pbs_script_path)
        else:
            if Path(pbs_script).is_file():
                pbs_script_path = pbs_script
            else:
                pbs_script_text = pbs_script
                with open(pbs_script_path, "w") as f:
                    f.write(pbs_script_text)

        # Transfer script to supercomputer using put_file()
        ret = await self.put_file(pbs_script_path, working_dir / remote_name)

        if "success" in ret and ret["success"] == "false":
            raise RuntimeError("An exception occurred while submitting job script: {}".format(ret["error"]))

        # Submit the script using call() with qsub command
        try:
            job_id = await self.call(f"qsub {remote_name}", working_dir=working_dir)
        except RuntimeError as e:
            raise RuntimeError("An exception occurred while submitting job script: {}".format(str(e)))

        # Clean up (remove temp files)
        os.remove(pbs_script_path)

        return job_id.strip()

    @_ensure_connected
    async def get_queues(self, update_cache=False):
        if self._queues is None or update_cache:
            self._queues = self._process_get_queues_output(await self.call("qstat -Q"))
        return self._queues

    @_ensure_connected
    async def get_raw_queue_stats(self):
        return json.loads(await self.call("qstat -Q -f -F json"))["Queue"]

    @_ensure_connected
    async def get_available_modules(self, flatten=False):
        return self._process_get_available_modules_output(await self.call("module avail"), flatten)

    @_ensure_connected
    async def get_loaded_modules(self):
        return self._process_get_loaded_modules_output(await self.call("module list"))

    async def _debug_uit(self, local_vars):
        """Show information about and around UIT+ calls for debug logging

        It can be called from any UIT Client method right after using requests.post().
        The recommended way to call this is:
            debug_start_time = time.perf_counter()
            r = requests.post(...)
            logger.debug(await self._debug_uit(locals()))

        It will not run if DEBUG logging is not enabled since this code is not perfect,
        and nobody wants to see debug log code cause exceptions in production.
        It doesn't run logger.debug() itself so that the parent function will show in the logs.
        """
        if not logger.isEnabledFor(logging.DEBUG):
            return

        try:
            resp = local_vars["r"].json()
            if inspect.iscoroutine(resp):
                resp = await resp
        except (RuntimeError, aiohttp.client_exceptions.ContentTypeError):
            # get_file only returns file contents, not json, so it always causes ContentTypeError.
            resp = {}

        return self._process_uit_debug(resp, local_vars)

    async def safe_close(self):
        if self._session is not None:
            await self.session.close()
            self._session = None
