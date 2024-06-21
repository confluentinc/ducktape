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
import boto3
import os
import base64
import winrm

from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5

from botocore.exceptions import ClientError

from ducktape.cluster.cluster_spec import WINDOWS
from ducktape.cluster.remoteaccount import RemoteAccount, RemoteCommandError


class WindowsRemoteAccount(RemoteAccount):
    """
    Windows remote accounts are currently only supported in EC2. See ``_setup_winrm()`` for how the WinRM password
    is fetched, which is currently specific to AWS.

    The Windows AMI needs to also have an SSH server running to support SSH commands, SCP, and rsync.
    """

    WINRM_USERNAME = "Administrator"

    def __init__(self, *args, **kwargs):
        super(WindowsRemoteAccount, self).__init__(*args, **kwargs)
        self.os = WINDOWS
        self._winrm_client = None

    @property
    def winrm_client(self):
        # TODO: currently this only works in AWS EC2 provisioned by Vagrant. Add support for other environments.

        # check if winrm has already been setup. If yes, return immediately.
        if self._winrm_client:
            return self._winrm_client

        # first get the instance ID of this machine from Vagrant's metadata.
        ec2_instance_id_path = os.path.join(os.getcwd(), ".vagrant", "machines", self.ssh_config.host, "aws", "id")
        instance_id_file = None
        try:
            instance_id_file = open(ec2_instance_id_path, 'r')
            ec2_instance_id = instance_id_file.read().strip()
            if not ec2_instance_id or ec2_instance_id == "":
                raise Exception
        except Exception:
            raise Exception("Could not extract EC2 instance ID from local file: %s" % ec2_instance_id_path)
        finally:
            if instance_id_file:
                instance_id_file.close()

        self._log(logging.INFO, "Found EC2 instance id: %s" % ec2_instance_id)

        # then get the encrypted password.
        client = boto3.client('ec2')
        try:
            response = client.get_password_data(InstanceId=ec2_instance_id)
        except ClientError as ce:
            if "InvalidInstanceID.NotFound" in str(ce):
                raise Exception("The instance id '%s' couldn't be found. Is the correct AWS region configured?"
                                % ec2_instance_id)
            else:
                raise ce

        self._log(logging.INFO, "Fetched encrypted winrm password and will decrypt with private key: %s"
                  % self.ssh_config.identityfile)

        # then decrypt the password using the private key.
        key_file = None
        try:
            key_file = open(self.ssh_config.identityfile, 'r')
            key = key_file.read()
            rsa_key = RSA.importKey(key)
            cipher = PKCS1_v1_5.new(rsa_key)
            winrm_password = cipher.decrypt(base64.b64decode(response["PasswordData"]), None)
            self._winrm_client = winrm.Session(self.ssh_config.hostname, auth=(WindowsRemoteAccount.WINRM_USERNAME,
                                                                               winrm_password))
        finally:
            if key_file:
                key_file.close()

        return self._winrm_client

    def fetch_externally_routable_ip(self, is_aws=None):
        # EC2 windows machines aren't given an externally routable IP. Use the hostname instead.
        return self.ssh_config.hostname

    def run_winrm_command(self, cmd, allow_fail=False):
        self._log(logging.DEBUG, "Running winrm command: %s" % cmd)
        result = self.winrm_client.run_cmd(cmd)
        if not allow_fail and result.status_code != 0:
            raise RemoteCommandError(self, cmd, result.status_code, result.std_err)
        return result.status_code
