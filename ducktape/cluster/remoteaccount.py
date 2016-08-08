# Copyright 2014 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
from paramiko import SSHClient, SSHConfig, AutoAddPolicy
import shutil
import signal
import socket
import stat
import tempfile
from contextlib import contextmanager

from ducktape.utils.http_utils import HttpMixin
from ducktape.utils.util import wait_until
from ducktape.errors import DucktapeError


class RemoteAccountError(DucktapeError):
    """This exception is raised when an attempted action on a remote node fails.
    """
    def __init__(self, account, msg):
        self.account_str = str(account)
        self.msg = msg

    def __str__(self):
        return "%s: %s" % (self.account_str, self.msg)


class RemoteCommandError(RemoteAccountError):
    """This exception is raised when a process run by ssh*() returns a non-zero exit status.
    """
    def __init__(self, account, cmd, exit_status, remote_err_msg):
        self.account_str = str(account)
        self.exit_status = exit_status
        self.cmd = cmd
        self.remote_err_msg = remote_err_msg

    def __str__(self):
        msg = "%s: Command '%s' returned non-zero exit status %d." % (self.account_str, self.cmd, self.exit_status)
        if self.remote_err_msg:
            msg += " Remote error message: %s" % self.remote_err_msg
        return msg


class RemoteAccount(HttpMixin):
    def __init__(self, hostname, user=None, ssh_args=None, ssh_hostname=None, externally_routable_ip=None, logger=None):
        self.hostname = hostname
        self.user = user
        self.ssh_args = ssh_args
        self.ssh_hostname = ssh_hostname
        self.externally_routable_ip = externally_routable_ip
        self._logger = logger
        self._ssh_config = None
        self._ssh_client = None
        self._sftp_client = None

    @property
    def logger(self):
        if self._logger:
            return self._logger
        else:
            return logging.getLogger(__name__)

    @logger.setter
    def logger(self, logger):
        self._logger = logger

    def _log(self, level, msg, *args, **kwargs):
        msg = "%s: %s" % (str(self), msg)
        self.logger.log(level, msg, *args, **kwargs)

    @property
    def ssh_config(self):
        if not self._ssh_config:
            self._ssh_config = self._parse_ssh_opts()
        return self._ssh_config

    @property
    def ssh_client(self):
        if not self._ssh_client:
            o = self.ssh_config.lookup(self.hostname)

            client = SSHClient()
            client.set_missing_host_key_policy(AutoAddPolicy())

            client.connect(
                hostname=o.get('hostname', self.hostname),
                port=int(o.get('port', 22)),
                username=self.user,
                password=None,
                key_filename=o.get('identityfile'),
                look_for_keys=False)
            self._ssh_client = client

        return self._ssh_client

    @property
    def sftp_client(self):
        if not self._sftp_client:
            self._sftp_client = self.ssh_client.open_sftp()

        # TODO: can we check that it is still open before returning it? Not sure how timeouts would work for a long-lived session?
        return self._sftp_client

    def close(self):
        """Close/release any outstanding network connections to remote account."""

        if self._ssh_client:
            self._ssh_client.close()
            self._ssh_client = None
        if self._sftp_client:
            self._sftp_client.close()
            self._sftp_client = None

    def _parse_ssh_opts(self):
        if self.ssh_args is None:
            return SSHConfig()

        args = self.ssh_args
        args = args.split("-o")
        args = [a.strip() for a in args]
        args = [a.replace("'", "") for a in args]
        args = [a.replace("\"", "") for a in args]
        args = [a.replace("\\", "") for a in args]
        args = [a for a in args if len(a) > 0]

        args_dict = {"Host": self.hostname}
        for a in args:
             pair = a.split(' ')
             args_dict[pair[0]] = pair[1]
        ssh_info_lines = ["%s %s" % (k, v) for k, v in args_dict.iteritems()]

        f = tempfile.NamedTemporaryFile(delete=False)
        try:
            f.write("\n".join(ssh_info_lines))
            f.close()

            config = SSHConfig()
            with open(f.name, "r") as fd:
                config.parse(fd)
        finally:
            if os.path.exists(f.name):
                os.remove(f.name)
        return config

    def __str__(self):
        r = ""
        if self.user:
            r += self.user + "@"
        r += self.hostname
        return r

    def __repr__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return other is not None and self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(tuple(sorted(self.__dict__.items())))

    @property
    def local(self):
        """Returns true if this 'remote' account is actually local. This is only a heuristic, but should work for simple local testing."""
        return self.hostname == "localhost" and self.user is None and self.ssh_args is None

    def wait_for_http_service(self, port, headers, timeout=20, path='/'):
        """Wait until this service node is available/awake."""
        url = "http://%s:%s%s" % (self.externally_routable_ip, str(port), path)

        err_msg = "Timed out trying to contact service on %s. " % url + \
                            "Either the service failed to start, or there is a problem with the url."
        wait_until(lambda: self._can_ping_url(url, headers), timeout_sec=timeout, backoff_sec=.25, err_msg=err_msg)

    def _can_ping_url(self, url, headers):
        """See if we can successfully issue a GET request to the given url."""
        try:
            self.http_request(url, "GET", "", headers, timeout=.75)
            return True
        except:
            return False

    def ssh_command(self, cmd):
        if self.local:
            return cmd
        r = "ssh "
        if self.user:
            r += self.user + "@"
        r += self.hostname + " "
        if self.ssh_args:
            r += self.ssh_args + " "
        r += "'" + cmd.replace("'", "'\\''") + "'"
        return r

    def ssh(self, cmd, allow_fail=False):
        """Run the given command on the remote host, and block until the command has finished running.

        :param cmd The remote ssh command
        :param allow_fail If True, ignore nonzero exit status of the remote command, else raise an RemoteCommandError

        :return The exit status of the command.
        :raise RemoteCommandError If allow_fail is False and the command returns a non-zero exit status, raises
            RemoteCommandError.
        """
        self._log(logging.DEBUG, "Running ssh command: %s" % cmd)

        client = self.ssh_client
        stdin, stdout, stderr = client.exec_command(cmd)

        exit_status = stdin.channel.recv_exit_status()
        try:
            if not allow_fail and exit_status != 0:
                raise RemoteCommandError(self, cmd, exit_status, stderr.read())
        finally:
            stdin.close()
            stdout.close()
            stderr.close()

        return exit_status

    def ssh_capture(self, cmd, allow_fail=False, callback=None):
        """Run the given command asynchronously via ssh, and return an SSHOutputIter object.

        Does *not* block

        :param cmd The remote ssh command
        :param allow_fail If True, ignore nonzero exit status of the remote command, else raise an RemoteCommandError
        :param callback If set, the iterator returns callback(line) for each line of output instead of the raw output

        :return SSHOutputIter object which allows iteration through each line of output.
        :raise RemoteCommandError If allow_fail is False and the command returns a non-zero exit status, raises
            RemoteCommandError.
        """
        self._log(logging.DEBUG, "Running ssh command: %s" % cmd)

        client = self.ssh_client
        stdin, stdout, stderr = client.exec_command(cmd)

        def output_generator():

            for line in iter(stdout.readline, ''):
                if callback is None:
                    yield line
                else:
                    yield callback(line)
            try:
                exit_status = stdin.channel.recv_exit_status()
                if not allow_fail and exit_status != 0:
                    raise RemoteCommandError(self, cmd, exit_status, stderr.read())
            finally:
                stdin.close()
                stdout.close()
                stderr.close()

        return SSHOutputIter(output_generator(), stdout)

    def ssh_output(self, cmd, allow_fail=False):
        """Runs the command via SSH and captures the output, returning it as a string.

        :param cmd The remote ssh command.
        :param allow_fail If True, ignore nonzero exit status of the remote command, else raise an RemoteCommandError

        :return The stdout output from the ssh command.
        :raise RemoteCommandError If allow_fail is False and the command returns a non-zero exit status, raises
            RemoteCommandError.
        """
        self._log(logging.DEBUG, "Running ssh command: %s" % cmd)

        client = self.ssh_client
        stdin, stdout, stderr = client.exec_command(cmd)

        try:
            stdoutdata = stdout.read()
            exit_status = stdin.channel.recv_exit_status()
            if not allow_fail and exit_status != 0:
                raise RemoteCommandError(self, cmd, exit_status, stderr.read())
        finally:
            stdin.close()
            stdout.close()
            stderr.close()

        return stdoutdata

    def alive(self, pid):
        """Return True if and only if process with given pid is alive."""
        try:
            self.ssh("kill -0 %s" % str(pid), allow_fail=False)
            return True
        except:
            return False

    def signal(self, pid, sig, allow_fail=False):
        cmd = "kill -%s %s" % (str(sig), str(pid))
        self.ssh(cmd, allow_fail=allow_fail)

    def kill_process(self, process_grep_str, clean_shutdown=True, allow_fail=False):
        cmd = """ps ax | grep -i """ + process_grep_str + """ | grep -v grep | awk '{print $1}'"""
        pids = [pid for pid in self.ssh_capture(cmd, allow_fail=True)]

        if clean_shutdown:
            sig = signal.SIGTERM
        else:
            sig = signal.SIGKILL

        for pid in pids:
            self.signal(pid, sig, allow_fail=allow_fail)

    def copy_between(self, src, dest, dest_node):
        """Copy src_path to dest_path on dest_node

        :param src_path Path to the file or directory we want to copy
        :param dest_path The destination path
        :param dest_node The node to which we want to copy the file/directory

        Note that if src is a directory, this will automatically copy recursively.

        Example:

        path/to/file, path/to/renamed_file, node
            file will be copied to renamed_file on node

        """
        # TODO: if dest is an existing file, what is the behavior?

        temp_dir = tempfile.mkdtemp()

        try:
            src_name = src
            if src_name.endswith(os.path.sep):
                src_name = src_name[:-len(os.path.sep)]  # trim off path separator from end
            src_name = os.path.basename(src_name)

            # TODO: deal with very unlikely case that src_name matches temp_dir name?
            # TODO: I think this actually works

            local_dest = os.path.join(temp_dir, src_name)
            self.copy_from(src, local_dest)

            dest_node.account.copy_to(local_dest, dest)

        finally:
            if os.path.isdir(temp_dir):
                shutil.rmtree(temp_dir)

    def copy_from(self, src, dest):
        if os.path.isdir(dest):
            # dest is an existing directory, so assuming src looks like path/to/src_name,
            # in this case we'll copy as:
            #   path/to/src_name -> dest/src_name
            src_name = src
            if src_name.endswith(os.path.sep):
                src_name = src_name[:-len(os.path.sep)]  # trim off path separator from end
            src_name = os.path.basename(src_name)

            dest = os.path.join(dest, src_name)

        if self.isfile(src):
            self.sftp_client.get(src, dest)
        elif self.isdir(src):
            # we can now assume dest path looks like: path_that_exists/new_directory
            os.mkdir(dest)

            # for obj in `ls src`, if it's a file, copy with copy_file_from, elif its a directory, call again
            for obj in self.sftp_client.listdir(src):
                obj_path = os.path.join(src, obj)
                if self.isfile(obj_path) or self.isdir(obj_path):
                    self.copy_from(obj_path, dest)
                else:
                    # TODO what about uncopyable file types?
                    pass

    def copy_to(self, src, dest):

        if self.isdir(dest):
            # dest is an existing directory, so assuming src looks like path/to/src_name,
            # in this case we'll copy as:
            #   path/to/src_name -> dest/src_name
            src_name = src
            if src_name.endswith(os.path.sep):
                src_name = src_name[:-len(os.path.sep)]  # trim off path separator from end
            src_name = os.path.basename(src_name)

            dest = os.path.join(dest, src_name)

        if os.path.isfile(src):
            # local to remote
            self.sftp_client.put(src, dest)
        elif os.path.isdir(src):
            # we can now assume dest path looks like: path_that_exists/new_directory
            self.mkdir(dest)

            # for obj in `ls src`, if it's a file, copy with copy_file_from, elif its a directory, call again
            for obj in os.listdir(src):
                obj_path = os.path.join(src, obj)
                if os.path.isfile(obj_path) or os.path.isdir(obj_path):
                    self.copy_to(obj_path, dest)
                else:
                    # TODO what about uncopyable file types?
                    pass

    def islink(self, path):
        try:
            # stat should follow symlinks
            path_stat = self.sftp_client.lstat(path)
            return stat.S_ISLNK(path_stat.st_mode)
        except:
            # TODO figure out which errors are legit (e.g. if file does not exist, what will sftp.stat do?)
            is_file = False

        return is_file

    def isdir(self, path):
        try:
            # stat should follow symlinks
            path_stat = self.sftp_client.stat(path)
            return stat.S_ISDIR(path_stat.st_mode)
        except:
            # TODO figure out which errors are legit (e.g. if file does not exist, what will sftp.stat do?)
            is_file = False

        return is_file

    def exists(self, path):
        """Test that the path exists, but don't follow symlinks."""
        try:
            # stat follows symlinks and tries to stat the actual file
            self.sftp_client.lstat(path)
            return True
        except IOError:
            return False

    def isfile(self, path):
        """Imitates semantics of os.path.isfile

        :path Path to the thing to check
        :return True iff path is a file or a symlink to a file, else False. Note False can mean path does not exist.
        """
        try:
            # stat should follow symlinks
            path_stat = self.sftp_client.stat(path)
            return stat.S_ISREG(path_stat.st_mode)
        except:
            # TODO figure out which errors are legit (e.g. if file does not exist, what will sftp.stat do?)
            is_file = False

        return is_file

    def open(self, path, mode='r'):
        return self.sftp_client.open(path, mode)

    def create_file(self, path, contents):
        """Create file at path, with the given contents.

        If the path already exists, it will be overwritten.
        """
        # TODO: what should semantics be if path exists? what actually happens if it already exists?
        # TODO: what happens if the base part of the path does not exist?
        with self.sftp_client.open(path, "w") as f:
            f.write(contents)

    def mkdir(self, path, mode=0755):

        self.sftp_client.mkdir(path, mode)

    def mkdirs(self, path, mode=0755):
        self.ssh("mkdir -p %s && chmod %o %s" % (path, mode, path))

    def remove(self, path, allow_fail=False):
        """Remove the given file or directory"""

        if not self.exists(path):
            msg = "Cannot remove %s because it does not exist." % path
            self._log(logging.DEBUG, msg)
            if not allow_fail:
                raise RemoteAccountError(self, msg)

        if not (self.isdir(path) or self.isfile(path) or self.islink(path)):
            msg = "Cannot remove %s because it is neither a directory, nor a path, nor a symlink." % path
            self._log(logging.DEBUG, msg)
            if not allow_fail:
                raise RemoteAccountError(self, msg)

        self.ssh("rm -rf %s" % path)

    @contextmanager
    def monitor_log(self, log):
        """
        Context manager that returns an object that helps you wait for events to
        occur in a log. This checks the size of the log at the beginning of the
        block and makes a helper object available with convenience methods for
        checking or waiting for a pattern to appear in the log. This will commonly
        be used to start a process, then wait for a log message indicating the
        process is in a ready state.

        See LogMonitor for more usage information.
        """
        try:
            offset = int(self.ssh_output("wc -c %s" % log).split()[0])
        except:
            offset = 0
        yield LogMonitor(self, log, offset)


class SSHOutputIter(object):
    """Helper class that wraps around an iterable object to provide has_next() in addition to next()
    """
    def __init__(self, iter_obj, channel_file=None):
        """
        :param iter_obj An iterator
        :param channel_file A paramiko ChannelFile object
        """
        self.iter_obj = iter_obj
        self.channel_file = channel_file

        # sentinel is used as an indicator that there is currently nothing cached
        # If self.cached is self.sentinel, then next object from ier_obj is not yet cached.
        self.sentinel = object()
        self.cached = self.sentinel

    def __iter__(self):
        return self

    def next(self):
        if self.cached is self.sentinel:
            return next(self.iter_obj)
        next_obj = self.cached
        self.cached = self.sentinel
        return next_obj

    def has_next(self, timeout_sec=None):
        """Return True iff next(iter_obj) would return another object within timeout_sec, else False.

        If timeout_sec is None, next(iter_obj) may block indefinitely.
        """
        assert timeout_sec is None or self.channel_file is not None, "should have descriptor to enforce timeout"

        prev_timeout = None
        if self.cached is self.sentinel:
            if self.channel_file is not None:
                prev_timeout = self.channel_file.channel.gettimeout()

                # when timeout_sec is None, next(iter_obj) will block indefinitely
                self.channel_file.channel.settimeout(timeout_sec)
            try:
                self.cached = next(self.iter_obj, self.sentinel)
            except socket.timeout:
                self.cached = self.sentinel
            finally:
                if self.channel_file is not None:
                    # restore preexisting timeout
                    self.channel_file.channel.settimeout(prev_timeout)

        return self.cached is not self.sentinel


class LogMonitor(object):
    """
    Helper class returned by monitor_log. Should be used as:

    with remote_account.monitor_log("/path/to/log") as monitor:
        remote_account.ssh("/command/to/start")
        monitor.wait_until("pattern.*to.*grep.*for", timeout_sec=5)

    to run the command and then wait for the pattern to appear in the log.
    """

    def __init__(self, acct, log, offset):
        self.acct = acct
        self.log = log
        self.offset = offset

    def wait_until(self, pattern, **kwargs):
        """
        Wait until the specified pattern is found in the log, after the initial
        offset recorded when the LogMonitor was created. Additional keyword args
        are passed directly to ducktape.utils.util.wait_until
        """
        return wait_until(lambda: self.acct.ssh("tail -c +%d %s | grep '%s'" % (self.offset+1, self.log, pattern), allow_fail=True) == 0, **kwargs)
