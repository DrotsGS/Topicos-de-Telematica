#!/usr/bin/env bash
# Prepara una instancia EC2 (Ubuntu 24.04) para correr DFSha.
# El Learner Lab apaga las instancias al cerrar sesion, asi que vas a
# ejecutar esto muchas veces. No lo hagas a mano.
set -e

sudo apt-get update
sudo apt-get install -y ca-certificates curl git

sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
     -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io \
     docker-buildx-plugin docker-compose-plugin

sudo usermod -a -G docker ubuntu

echo
echo "Docker instalado. Cierra la sesion SSH y vuelve a entrar para que"
echo "el grupo docker tome efecto."
echo
echo "Recuerda abrir en el Security Group:"
echo "  50051/tcp  NameNode"
echo "  50060/tcp  DataNode"
