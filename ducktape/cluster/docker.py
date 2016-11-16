# Copyright 2016 Confluent Inc.
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

from __future__ import absolute_import, print_function
from .cluster import Cluster, ClusterSlot
from .remoteaccount import RemoteAccount
from ducktape.errors import DucktapeError

import collections, json, os, subprocess, six


class DockerCluster(Cluster):
    """
    An implementation of Cluster that uses Docker containers.
    """

    def __init__(self):
        super(DockerCluster, self).__init__()

        cids = subprocess.check_output('docker ps -q'.split(), stderr=subprocess.STDOUT).split()
        init_nodes = []
        for cinfo in json.loads(subprocess.check_output(['docker', 'inspect'] + cids, stderr=subprocess.STDOUT)):
            networks = list(cinfo["NetworkSettings"]["Networks"].items())
            if len(networks) != 1:
                raise DucktapeError("DockerCluster only supports containers with 1 network.")
            # Currently SSH
            ssh_hostname = "localhost"
            ssh_port = int((x for x in cinfo["NetworkSettings"]["Ports"]["22/tcp"] if x["HostIp"] == "0.0.0.0").next()["HostPort"])
            # We'll use the IPAddress as the hostname because there
            # doesn't seem to be an easy way to setup hostnames that
            # work across containers
            ip_addr = networks[0][1]["IPAddress"]
            init_nodes.append(RemoteAccount(hostname=ip_addr, user="root", ssh_hostname=ssh_hostname,
                                            ssh_args="-o 'HostName localhost' -o 'Port %s' -o 'UserKnownHostsFile /dev/null' -o 'StrictHostKeyChecking no' -o 'PasswordAuthentication no' -o 'IdentityFile /home/vagrant/.ssh/id_rsa' -o 'IdentitiesOnly yes' -o 'LogLevel FATAL'" % ssh_port))
        self.available_nodes = collections.deque(init_nodes)
        self.in_use_nodes = set()
        self.id_source = 1

    def __len__(self):
        return len(self.available_nodes) + len(self.in_use_nodes)

    def num_available_nodes(self):
        return len(self.available_nodes)

    def request(self, nslots):
        if nslots > self.num_available_nodes():
            err_msg = "There aren't enough available nodes to satisfy the resource request. " \
                "Total cluster size: %d, Requested: %d, Already allocated: %d, Available: %d. " % \
                      (len(self), nslots, len(self.in_use_nodes), self.num_available_nodes())
            err_msg += "Make sure your cluster has enough nodes to run your test or service(s)."
            raise RuntimeError(err_msg)

        result = []
        for i in range(nslots):
            node = self.available_nodes.popleft()
            result.append(ClusterSlot(self, node, slot_id=self.id_source))
            self.in_use_nodes.add(self.id_source)
            self.id_source += 1
        return result

    def free_single(self, slot):
        assert(slot.slot_id in self.in_use_nodes)
        self.in_use_nodes.remove(slot.slot_id)
        self.available_nodes.append(slot.account)


def validate_config(config):
    DEFAULTS = {
        "machine": "ducktape",
        # docker-machine settings are prefixed with "machine-"
        "machine-driver": "virtualbox",

        "image": "kafkatest",
        "dockerfile": "Dockerfile",
        "network": "kafkatestnet",
    }
    REQUIRED_KEYS = list(DEFAULTS.keys()) + ["size"]

    for k,v_default in six.iteritems(DEFAULTS):
        config[k] = config.get(k, v_default)
    for k in REQUIRED_KEYS:
        v = config.get(k, None)
        if v is None or v == "":
            raise DucktapeError("Invalid config: empty value for required key " + k)


def ensure_machine(config):
    """
    Tries to ensure that if a docker daemon is not available, that a docker machine is started to provide one.
    """

    # First try to check if there is an accessible docker daemon running.
    try:
        status(config['size'])
        return
    except subprocess.CalledProcessError:
        pass  # Fall through to create machine

    print("Docker daemon not found, trying to create docker-machine")
    try:
        machine_keys = [k.split('machine-', 1)[1] for k in config.iterkeys() if k.startswith('machine-')]
        machine_flags = ['--%s=%s' % (k, config[k]) for k in machine_keys]
        subprocess.check_call(['docker-machine', 'create'] + machine_flags + [config['machine']])
    except subprocess.CalledProcessError as e:
        raise DucktapeError("Failed to create docker-machine", e)


def build(dockerfile, image):
    print("Building Docker image", image)
    try:
        subprocess.check_call(['docker', 'build', '-f', dockerfile, '-t', image, '.'])
    except subprocess.CalledProcessError as e:
        raise DucktapeError("Failed to build Docker image", e)


def ensure_network(name):
    print("Checking whether network", name, "exists")
    try:
        subprocess.check_output(['docker', 'network', 'inspect', name], stderr=subprocess.STDOUT)
        return
    except subprocess.CalledProcessError:
        pass  # Fall through to create the network

    print("Creating network", name)
    try:
        subprocess.check_output(['docker', 'network', 'create', '--driver=bridge', name], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        raise DucktapeError("Failed to validate or create Docker network", e)


def worker_names(count):
    return ["worker" + str(i) for i in range(count)]


def up(image, network, size):
    print("Starting cluster of", size, image, "containers on", network)
    statuses = status(size)

    for name in worker_names(size):
        if statuses[name]["status"] == "running":
            continue  # FIXME paused, other non-running states that shouldn't just create a new container?
        subprocess.check_output(['docker', 'run',
                                 '--net=' + network,
                                 '--detach',
                                 '--publish-all',
                                 '--volume=%s:/kafka-src' % os.getcwd(),  # FIXME
                                 '--name=' + name,
                                 image])


def status(size):
    """Get list of currently running worker containers."""
    # Get any running containers that match our naming scheme
    active_names = [line.split()[-1] for line in subprocess.check_output(['docker', 'ps', '-a']).split('\n')[1:] if line.strip()]
    active_names = set([name for name in active_names if name.startswith('worker')]) # FIXME worker constant?

    unstarted_state = {"status": "stopped"}
    try:
        active_names_ordered = list(active_names)
        cinfos = json.loads(subprocess.check_output(['docker', 'inspect'] + active_names_ordered)) if active_names_ordered else {}
    except ValueError:
        raise RuntimeError("Could not parse docker inspect output when trying to determine status for %s" % active_names)

    active_indexes = {name:idx for idx,name in enumerate(active_names_ordered)}
    return {name: {"status": cinfos[active_indexes[name]]["State"]["Status"]} if name in active_indexes else unstarted_state for name in worker_names(size)}


def print_status(size):
    statuses = status(size)
    print("Name\tStatus")
    for name in sorted(statuses.keys()):
        s = statuses[name]
        print("%s\t%s" % (name, s["status"]))


def down(size):
    """
    Stop any currently running containers.

    This only stops the containers but does not remove them.
    """
    nodes = status(size)
    subprocess.check_output(['docker', 'kill'] + nodes.keys())


def main():
    """ducktape-docker - control program for Docker clusters used for Ducktape tests

    This command allows you to setup, bootstrap, and destroy clusters
    of Docker containers to use for Ducktape tests. While the ducktape
    command itself only manages running tests and assumes the cluster
    already exists, this handles other steps related to cluster
    management for Docker clusters.

    This tool is driven by a JSON config file, .docker-cluster, that
    should be stored in the directory you run ducktape-docker from. It
    can contain the following settings:

    * image - the name of the Docker image tag to use when building
      the Docker image and starting containers (Default: kafkatest)
    * dockerfile - the path to the Dockerfile to use to build the
      Docker image. (Default: Dockerfile)
    * network - the name of the bridge network to run the containers
      on (Default: kafkatestnet)
    * size - number of containers to run
    """

    from argparse import ArgumentParser, RawTextHelpFormatter
    import sys

    parser = ArgumentParser(description=main.__doc__, formatter_class=RawTextHelpFormatter)
    parser.add_argument('command', type=str)

    args = parser.parse_args()

    try:
        config = json.load(open('.docker-cluster'))
    except IOError:
        print("Couldn't find .docker-cluster configuration file.")
        sys.exit(1)

    validate_config(config)
    ensure_machine(config)
    build(config["dockerfile"], config["image"])
    ensure_network(config["network"])
    up(config["image"], config["network"], config["size"])
    print_status(config["size"])
    down(config["size"])
