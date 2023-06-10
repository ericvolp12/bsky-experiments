#!/bin/bash

# Update the instance
sudo apt-get update -y
sudo apt-get upgrade -y

# Install Docker
sudo apt-get install apt-transport-https ca-certificates curl gnupg lsb-release -y
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update -y
sudo apt-get install docker-ce docker-ce-cli containerd.io -y

# Add 'user' to docker group so we can execute docker commands without sudo
sudo usermod -aG docker user

# Install Git
sudo apt-get install git btop -y

# Install Go
curl -LO https://golang.org/dl/go1.20.5.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.20.5.linux-amd64.tar.gz
rm go1.20.5.linux-amd64.tar.gz

# Download required docker images
docker pull redis/redis-stack-server:latest
docker pull docker.dragonflydb.io/dragonflydb/dragonfly:latest

# Clone the git repo for redis testing
git clone https://github.com/ericvolp12/bsky-experiments.git /home/user/bsky-experiments
cd /home/user/bsky-experiments

# Build the redis tester
/usr/local/go/bin/go build -o redis-tester cmd/redis-tester/main.go

# Chown the repo to user
sudo chown -R user:user /home/user/bsky-experiments
