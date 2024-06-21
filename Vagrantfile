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
# -*- mode: ruby -*-
# vi: set ft=ruby :

require 'socket'

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

# General config
enable_dns = false
num_workers = 3
ram_megabytes = 300
base_box = "ubuntu/focal64"

local_config_file = File.join(File.dirname(__FILE__), "Vagrantfile.local")
if File.exists?(local_config_file) then
  eval(File.read(local_config_file), binding, "Vagrantfile.local")
end

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.hostmanager.enabled = true
  config.hostmanager.manage_host = enable_dns
  config.hostmanager.include_offline = false

  ## Provider-specific global configs
  config.vm.provider :virtualbox do |vb,override|
    override.vm.box = base_box

    override.hostmanager.ignore_private_ip = false

    # Brokers started with the standard script currently set Xms and Xmx to 1G,
    # plus we need some extra head room.
    vb.customize ["modifyvm", :id, "--memory", ram_megabytes.to_s]
  end

  ## Cluster definition
  (1..num_workers).each { |i|
    name = "ducktape" + i.to_s
    config.vm.define name do |worker|
      worker.vm.hostname = name
      worker.vm.network :private_network, ip: "192.168.56." + (150 + i).to_s
    end
  }

end
