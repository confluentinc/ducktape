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

import os
import select
import signal
import subprocess
import tempfile
from contextlib import contextmanager

from ducktape.utils.http_utils import HttpMixin
from ducktape.utils.util import wait_until


class RemoteAccount(HttpMixin):
    def __init__(self, hostname, user=None, ssh_args=None, ssh_hostname=None, externally_routable_ip = None, logger=None):
        self.hostname = hostname
        self.user = user
        self.ssh_args = ssh_args
        self.ssh_hostname = ssh_hostname
        self.externally_routable_ip = externally_routable_ip
        self.logger = logger

    def __str__(self):
        r = ""
        if self.user:
            r += self.user + "@"
        r += self.hostname
        return r

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
        """
        Run the specified command on the remote host. If allow_fail is False and
        the command returns a non-zero exit status, throws
        subprocess.CalledProcessError. If allow_fail is True, returns the exit
        status of the command.
        """
        return self._ssh_quiet(self.ssh_command(cmd), allow_fail)

    def ssh_capture(self, cmd, allow_fail=False, callback=None):
        '''Runs the command via SSH and captures the output, yielding lines of the output.'''
        ssh_cmd = self.ssh_command(cmd)
        proc = subprocess.Popen(ssh_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)

        def output_generator():
            for line in iter(proc.stdout.readline, ''):
                if callback is None:
                    yield line
                else:
                    yield callback(line)
            proc.communicate()
            if proc.returncode != 0 and not allow_fail:
                raise subprocess.CalledProcessError(proc.returncode, ssh_cmd)

        return iter_wrapper(output_generator(), proc.stdout)

    def ssh_output(self, cmd, allow_fail=False):
        '''Runs the command via SSH and captures the output, returning it as a string.'''
        ssh_cmd = self.ssh_command(cmd)
        proc = subprocess.Popen(ssh_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
        (stdoutdata, stderrdata) = proc.communicate()
        if proc.returncode != 0 and not allow_fail:
            raise subprocess.CalledProcessError(proc.returncode, ssh_cmd)
        if self.logger is not None:
            self.logger.debug("Ran cmd " + ssh_cmd + " -> " + str(stdoutdata))
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

    def scp_from_command(self, src, dest, recursive=False):
        if self.user:
            remotehost = self.user + "@" + self.hostname
        else:
            remotehost = self.hostname

        if isinstance(src, basestring):
            src = remotehost + ":" + src
        else:
            # assume src is iterable
            # e.g. "ubuntu@host:path1 ubuntu@host:path2"
            src = " ".join([remotehost + ":" + path for path in src])

        r = "scp "
        if self.ssh_args:
            r += self.ssh_args + " "
        if recursive:
            r += "-r "

        r += src + " " + dest
        return r

    def scp_from(self, src, dest, recursive=False):
        """Copy something from this node. src may be a string or an iterable of several sources."""
        return self._ssh_quiet(self.scp_from_command(src, dest, recursive))

    def scp_to_command(self, src, dest, recursive=False):
        if not isinstance(src, basestring):
            # Assume src is iterable
            src = " ".join(src)

        r = "scp "
        if self.ssh_args:
            r += self.ssh_args + " "
        if recursive:
            r += "-r "
        r += src + " "
        if self.user:
            r += self.user + "@"
        r += self.hostname + ":" + dest
        return r

    def scp_to(self, src, dest, recursive=False):
        return self._ssh_quiet(self.scp_to_command(src, dest, recursive))

    def rsync_to_command(self, flags, src_dir, dest_dir):
        r = "rsync "
        if self.ssh_args:
            r += "-e \"ssh " + self.ssh_args + "\" "
        if flags:
            r += flags
        r += src_dir
        if self.user:
            r += self.user + "@"
        r += self.hostname + ":" + dest_dir
        return r

    def rsync_to(self, flags, src_dir, dest_dir):
        return self._ssh_quiet(self.rsync_to_command(flags, src_dir, dest_dir))

    def create_file(self, path, contents):
        tmp = tempfile.NamedTemporaryFile(delete=False)
        local_name = tmp.name
        tmp.write(contents)
        tmp.close()
        self.scp_to(local_name, path)
        os.remove(local_name)

    def _ssh_quiet(self, cmd, allow_fail=False):
        """Runs the command on the remote host using SSH and blocks until the remote command finishes.

        If it succeeds, there is no output; if it fails the output is printed
        and the CalledProcessError is re-raised. If allow_fail is True, the
        CalledProcess error will not be re-raised and the exit status of the
        process will be returned instead.
        """
        try:
            if self.logger is not None:
                self.logger.debug("Trying to run remote command: " + cmd)
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            return 0
        except subprocess.CalledProcessError as e:
            # Depending on the user, failure of remote commands may be part of normal usage patterns (e.g. if used in
            # a "wait_until" loop). So, log it, but don't make it too scary.
            if self.logger is not None:
                self.logger.debug("Error running remote command: " + cmd)
                self.logger.debug(e.output)

            if allow_fail:
                return e.returncode
            raise e

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
        except subprocess.CalledProcessError:
            offset = 0
        yield LogMonitor(self, log, offset)


class iter_wrapper(object):
    """
    Helper class that wraps around an iterable object to provide has_next() in addition to next()
    """
    def __init__(self, iter_obj, descriptor=None):
        self.iter_obj = iter_obj
        self.descriptor = descriptor
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
        """Check if output is available and possibly wait until the availability is known.
        1) If there is output available, return true
        2) If there is no output available (i.e. EOF), return false
        3) If it is unknown whether output is available
           - If timeout_sec is not specified, wait indefinitely until the availability is known
           - Otherwise, wait for at most time_sec seconds
             - If availability is known before timeout, return the true/false as appropriate
             - Otherwise, return false
        """
        assert timeout_sec is None or self.descriptor is not None, "should have descriptor to enforce timeout"

        if self.cached is self.sentinel:
            if timeout_sec is None or select.select([self.descriptor], [], [], timeout_sec)[0]:
                self.cached = next(self.iter_obj, self.sentinel)
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
