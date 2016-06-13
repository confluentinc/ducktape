# Copyright 2015 Confluent Inc.
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

from __future__ import absolute_import
from .cluster import Cluster, ClusterSlot
from .remoteaccount import RemoteAccount
from ducktape.errors import DucktapeError

import collections, json, os, subprocess

class DockerCluster(Cluster):
    """
    An implementation of Cluster that uses Docker containers.
    """

    def __init__(self, cluster_json=None):
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
        "image": "kafkatest",
        "dockerfile": "Dockerfile",
        "network": "kafkatestnet",
    }
    REQUIRED_KEYS = DEFAULTS.keys() + ["size"]

    for k,v_default in DEFAULTS.iteritems():
        config[k] = config.get(k, v_default)
    for k in REQUIRED_KEYS:
        v = config.get(k, None)
        if v is None or v == "":
            raise DucktapeError("Invalid config: empty value for required key " + k)

def build(dockerfile, image):
    print "Building Docker image", image
    try:
        subprocess.check_call(['docker', 'build', '-f', dockerfile, '-t', image, '.'])
    except subprocess.CalledProcessError:
        raise DucktapeError("Failed to build Docker image", e)

def ensure_network(name):
    print "Checking whether network", name, "exists"
    try:
        subprocess.check_output(['docker', 'network', 'inspect', name], stderr=subprocess.STDOUT)
        return
    except subprocess.CalledProcessError:
        pass  # Fall through to create the network
    
    print "Creating network", name
    try:
        subprocess.check_output(['docker', 'network', 'create', '--driver=bridge', name], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        raise DucktapeError("Failed to validate or create Docker network", e)

def up(image, network, size):
    print "Starting cluster with of", size, image, "containers on", network
    nodes = status()

    for i in range(size):
        name = "worker" + str(i) # FIXME
        if name in nodes: continue
        subprocess.check_output(['docker', 'run', 
                                 '--net=' + network,
                                 '--detach',
                                 '--publish-all',
                                 '--volume=%s:/kafka-src' % os.getcwd(), # FIXME
                                 '--name=' + name,
                                 image])

def status():
    """Get list of currently running worker containers."""
    try:
        subprocess.check_output(['docker', 'inspect'] + )

    # Get any running containers that match our naming scheme
    for x in [line for line in subprocess.check_output(['docker', 'ps']).split('\n')[1:] if line.strip()]:
        print repr(x)
    names = [line.split('\t')[6] for line in subprocess.check_output(['docker', 'ps']).split('\n')[1:] if line.strip()]
    names = set([name for name in names if name.startswith('worker')]) # FIXME worker constant?
    return names

def down():
    """
    Stop any currently running containers.
    
    This only stops the containers but does not remove them.
    """
    nodes = status()
    subprocess.check_output(['docker', 'kill'] + nodes)

def main():
    """ducktape-docker - control program for Docker clusters used for Ducktape tests

    ducktape-docker 

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

    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('command', type=str)

    config = json.load(open('.docker-cluster'))

    #validate_config(config)
    #build(config["dockerfile"], config["image"])
    #ensure_network(config["network"])
    #up(config["image"], config["network"], config["size"])
    print status()
    #down()
