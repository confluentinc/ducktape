# An image of ducktape that can be used to setup a Docker cluster where ducktape is run inside the container.

FROM ubuntu:14.04

ARG DUCKTAPE_VERSION=0.7.3

RUN apt-get update && \
    apt-get install -y libffi-dev libssl-dev openssh-server python-dev python-pip python-virtualenv && \
    virtualenv /opt/ducktape && \
    . /opt/ducktape/bin/activate && \
    pip install -U pip==9.0.3 setuptools wheel && \
    pip install bcrypt ducktape==$DUCKTAPE_VERSION cryptography==2.2.2 pynacl && \
    ln -s /opt/ducktape/bin/ducktape /usr/local/bin/ducktape && \
    mkdir /var/run/sshd && \
    mkdir /root/.ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

EXPOSE 22

CMD    ["/usr/sbin/sshd", "-D"]
