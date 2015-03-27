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

import os, subprocess, tempfile, time
import urllib2

class RemoteAccount(object):
    def __init__(self, hostname, user=None, ssh_args=None, java_home="default", kafka_home="default"):
        self.hostname = hostname
        self.user = user
        self.ssh_args = ssh_args
        self.java_home = java_home
        self.kafka_home = kafka_home

    @property
    def local(self):
        "Returns true if this 'remote' account is actually local. This is only a heuristic, but should work for simple local testing."
        return self.hostname == "localhost" and self.user is None and self.ssh_args is None

    def wait_for_http_service(self, port, headers, timeout=20, path='/'):
        url = "http://%s:%s%s" % (self.hostname, str(port), path)

        stop = time.time() + timeout
        awake = False
        while time.time() < stop:
            try:
                http_request(url, "GET", "", headers)
                awake = True
                break
            except:
                time.sleep(.25)
                pass
        if not awake:
            raise Exception("Timed out trying to contact service on %s. " % url +
                            "Either the service failed to start, or there is a problem with the url. "
                            "You may need to open Vagrantfile.local and add the line 'enable_dns = true'.")


    def ssh_command(self, cmd):
        r = "ssh "
        if self.user:
            r += self.user + "@"
        r += self.hostname + " "
        if self.ssh_args:
            r += self.ssh_args + " "
        r += "'" + cmd.replace("'", "'\\''") + "'"
        return r

    def ssh(self, cmd, allow_fail=False):
        return self._ssh_quiet(self.ssh_command(cmd), allow_fail)

    def ssh_capture(self, cmd):
        '''Runs the command via SSH and captures the output, yielding lines of the output.'''
        ssh_cmd = self.ssh_command(cmd)
        proc = subprocess.Popen(ssh_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in iter(proc.stdout.readline, ''):
            yield line
        proc.communicate()
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, ssh_cmd)

    def kill_process(self, process_grep_str, clean_shutdown=True, allow_fail=False):
        cmd = """ps ax | grep -i """ + process_grep_str + """ | grep java | grep -v grep | awk '{print $1}'"""
        pids = list(self.ssh_capture(cmd))

        if clean_shutdown:
            kill = "kill "
        else:
            kill = "kill -9 "

        for pid in pids:
            cmd = kill + pid
            self.ssh(cmd, allow_fail)

    def scp_from_command(self, src, dest, recursive=False):
        r = "scp "
        if self.ssh_args:
            r += self.ssh_args + " "
        if recursive:
            r += "-r "
        if self.user:
            r += self.user + "@"
        r += self.hostname + ":" + src + " " + dest
        return r

    def scp_from(self, src, dest, recursive=False):
        return self._ssh_quiet(self.scp_from_command(src, dest, recursive))

    def scp_to_command(self, src, dest, recursive=False):
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
        '''Runs the command on the remote host using SSH. If it succeeds, there is no
        output; if it fails the output is printed and the CalledProcessError is re-raised.'''
        try:
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            if allow_fail:
                return
            print "Error running remote command: " + cmd
            print e.output
            raise e

    def __str__(self):
        r = ""
        if self.user:
            r += self.user + "@"
        r += self.hostname
        return r


def http_request(url, method, data="", headers=None):
    if url[0:7].lower() != "http://":
        url = "http://%s" % url

    req = urllib2.Request(url, data, headers)
    req.get_method = lambda: method
    return urllib2.urlopen(req)
