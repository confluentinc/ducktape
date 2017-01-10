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

from contextlib import contextmanager
import logging
import os
from paramiko import SSHClient, MissingHostKeyPolicy
import shutil
import signal
import socket
import stat
import tempfile
import warnings

from ducktape.utils.util import wait_until
from ducktape.cluster.remoteaccount import RemoteAccount, RemoteCommandError


class IgnoreMissingHostKeyPolicy(MissingHostKeyPolicy):
    """Policy for ignoring missing host keys.
    Many examples show use of AutoAddPolicy, but this clutters up the known_hosts file unnecessarily.
    """
    def missing_host_key(self, client, hostname, key):
        return


class LinuxRemoteAccount(RemoteAccount):

    def __init__(self, ssh_config, externally_routable_ip=None, logger=None):
        super(LinuxRemoteAccount, self).__init__(ssh_config, externally_routable_ip=externally_routable_ip,
                                                 logger=logger)
        self._ssh_client = None
        self._sftp_client = None
        self.os = RemoteAccount.LINUX

    @property
    def ssh_config(self):
        return self.remote_command_config

    @property
    def ssh_client(self):
        if not self._ssh_client:
            client = SSHClient()
            client.set_missing_host_key_policy(IgnoreMissingHostKeyPolicy())

            client.connect(
                hostname=self.ssh_config.hostname,
                port=self.ssh_config.port,
                username=self.ssh_config.user,
                password=self.ssh_config.password,
                key_filename=self.ssh_config.identityfile,
                look_for_keys=False)
            self._ssh_client = client

        return self._ssh_client

    @property
    def sftp_client(self):
        if not self._sftp_client:
            self._sftp_client = self.ssh_client.open_sftp()

        return self._sftp_client

    def close(self):
        """Close/release any outstanding network connections to remote account."""

        if self._ssh_client:
            self._ssh_client.close()
            self._ssh_client = None
        if self._sftp_client:
            self._sftp_client.close()
            self._sftp_client = None

    def __str__(self):
        r = ""
        if self.user:
            r += self.user + "@"
        r += self.hostname
        return r

    @property
    def local(self):
        """Returns True if this 'remote' account is probably local.
        This is an imperfect heuristic, but should work for simple local testing."""
        return self.hostname == "localhost" and self.user is None and self.ssh_config is None

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

        # Unfortunately we need to read over the channel to ensure that recv_exit_status won't hang. See:
        # http://docs.paramiko.org/en/2.0/api/channel.html#paramiko.channel.Channel.recv_exit_status
        stdout.read()
        exit_status = stdout.channel.recv_exit_status()
        try:
            if not allow_fail and exit_status != 0:
                raise RemoteCommandError(self, cmd, exit_status, stderr.read())
        finally:
            stdin.close()
            stdout.close()
            stderr.close()

        return exit_status

    def ssh_capture(self, cmd, allow_fail=False, callback=None, combine_stderr=True, timeout_sec=None):
        """Run the given command asynchronously via ssh, and return an SSHOutputIter object.

        Does *not* block

        :param cmd The remote ssh command
        :param allow_fail If True, ignore nonzero exit status of the remote command, else raise an RemoteCommandError
        :param callback If set, the iterator returns callback(line) for each line of output instead of the raw output
        :param combine_stderr If True, return output from both stderr and stdout of the remote process.
        :param timeout_sec Set timeout on blocking reads/writes. Default None. For more details see
            http://docs.paramiko.org/en/2.0/api/channel.html#paramiko.channel.Channel.settimeout

        :return SSHOutputIter object which allows iteration through each line of output.
        :raise RemoteCommandError If allow_fail is False and the command returns a non-zero exit status, raises
            RemoteCommandError.
        """
        self._log(logging.DEBUG, "Running ssh command: %s" % cmd)

        client = self.ssh_client
        chan = client.get_transport().open_session(timeout=timeout_sec)

        chan.settimeout(timeout_sec)
        chan.exec_command(cmd)
        chan.set_combine_stderr(combine_stderr)

        stdin = chan.makefile('wb', -1)  # set bufsize to -1
        stdout = chan.makefile('r', -1)
        stderr = chan.makefile_stderr('r', -1)

        def output_generator():

            for line in iter(stdout.readline, ''):

                if callback is None:
                    yield line
                else:
                    yield callback(line)
            try:
                exit_status = stdout.channel.recv_exit_status()
                if not allow_fail and exit_status != 0:
                    raise RemoteCommandError(self, cmd, exit_status, stderr.read())
            finally:
                stdin.close()
                stdout.close()
                stderr.close()

        return SSHOutputIter(output_generator(), stdout)

    def ssh_output(self, cmd, allow_fail=False, combine_stderr=True, timeout_sec=None):
        """Runs the command via SSH and captures the output, returning it as a string.

        :param cmd The remote ssh command.
        :param allow_fail If True, ignore nonzero exit status of the remote command, else raise an RemoteCommandError
        :param combine_stderr If True, return output from both stderr and stdout of the remote process.
        :param timeout_sec Set timeout on blocking reads/writes. Default None. For more details see
            http://docs.paramiko.org/en/2.0/api/channel.html#paramiko.channel.Channel.settimeout

        :return The stdout output from the ssh command.
        :raise RemoteCommandError If allow_fail is False and the command returns a non-zero exit status, raises
            RemoteCommandError.
        """
        self._log(logging.DEBUG, "Running ssh command: %s" % cmd)

        client = self.ssh_client
        chan = client.get_transport().open_session(timeout=timeout_sec)

        chan.settimeout(timeout_sec)
        chan.exec_command(cmd)
        chan.set_combine_stderr(combine_stderr)

        stdin = chan.makefile('wb', -1)  # set bufsize to -1
        stdout = chan.makefile('r', -1)
        stderr = chan.makefile_stderr('r', -1)

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
        """Copy src to dest on dest_node

        :param src Path to the file or directory we want to copy
        :param dest The destination path
        :param dest_node The node to which we want to copy the file/directory

        Note that if src is a directory, this will automatically copy recursively.

        Example:

        path/to/file, path/to/renamed_file, node
            file will be copied to renamed_file on node

        """
        # TODO: if dest is an existing file, what is the behavior?

        temp_dir = tempfile.mkdtemp()

        try:
            # TODO: deal with very unlikely case that src_name matches temp_dir name?
            # TODO: I think this actually works
            local_dest = self._re_anchor_basename(src, temp_dir)

            self.copy_from(src, local_dest)

            dest_node.account.copy_to(local_dest, dest)

        finally:
            if os.path.isdir(temp_dir):
                shutil.rmtree(temp_dir)

    def scp_from(self, src, dest, recursive=False):
        warnings.warn("scp_from is now deprecated. Please use copy_from")
        self.copy_from(src, dest)

    def _re_anchor_basename(self, path, directory):
        """Anchor the basename of path onto the given directory

        Helper for the various copy_* methods.

        :param path Path to a file or directory. Could be on the driver machine or a worker machine.
        :param directory Path to a directory. Could be on the driver machine or a worker machine.

        Example:
            path/to/the_basename, another/path/ -> another/path/the_basename
        """
        path_basename = path

        # trim off path separator from end of path
        # this is necessary because os.path.basename of a path ending in a separator is an empty string
        # For example:
        #   os.path.basename("the/path/") == ""
        #   os.path.basename("the/path") == "path"
        if path_basename.endswith(os.path.sep):
            path_basename = path_basename[:-len(os.path.sep)]
        path_basename = os.path.basename(path_basename)

        return os.path.join(directory, path_basename)

    def copy_from(self, src, dest):
        if os.path.isdir(dest):
            # dest is an existing directory, so assuming src looks like path/to/src_name,
            # in this case we'll copy as:
            #   path/to/src_name -> dest/src_name
            dest = self._re_anchor_basename(src, dest)

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

    def scp_to(self, src, dest, recursive=False):
        warnings.warn("scp_to is now deprecated. Please use copy_to")
        self.copy_to(src, dest)

    def copy_to(self, src, dest):

        if self.isdir(dest):
            # dest is an existing directory, so assuming src looks like path/to/src_name,
            # in this case we'll copy as:
            #   path/to/src_name -> dest/src_name
            dest = self._re_anchor_basename(src, dest)

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
            return False

    def isdir(self, path):
        try:
            # stat should follow symlinks
            path_stat = self.sftp_client.stat(path)
            return stat.S_ISDIR(path_stat.st_mode)
        except:
            return False

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
            return False

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

        if allow_fail:
            cmd = "rm -rf %s" % path
        else:
            cmd = "rm -r %s" % path

        self.ssh(cmd, allow_fail=allow_fail)

    def fetch_externally_routable_ip(self, is_aws):
        if is_aws:
            cmd = "/sbin/ifconfig eth0 "
        else:
            cmd = "/sbin/ifconfig eth1 "
        cmd += "| grep 'inet addr' | tail -n 1 | egrep -o '[0-9\.]+' | head -n 1 2>&1"
        output = "".join(self.ssh_capture(cmd))
        return output.strip()

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
