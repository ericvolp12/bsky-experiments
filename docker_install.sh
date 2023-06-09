#!/bin/bash
apt update
apt upgrade -y
apt install ca-certificates gnupg lsb-release git curl iputils-ping -y

mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o
/etc/apt/keyrings/docker.gpg
echo \
"deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
$(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

chmod a+r /etc/apt/keyrings/docker.gpg
apt-get update
apt-get upgrade -y
apt-get install docker-ce docker-ce-cli containerd.io -y
exec "$@"
