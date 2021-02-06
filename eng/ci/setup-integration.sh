#!/bin/bash
set -euo pipefail

sudo snap switch lxd --channel=latest
sudo snap refresh lxd
sudo lxd init --preseed <<< "
config:
  images.auto_update_interval: "0"
networks:
- config:
    ipv4.address: auto
    ipv6.address: none
  description: ""
  name: lxdbr0
  type: ""
storage_pools:
- config: {}
  description: ""
  name: default
  driver: dir
profiles:
- config: {}
  description: ""
  devices:
    eth0:
      name: eth0
      network: lxdbr0
      type: nic
    root:
      path: /
      pool: default
      type: disk
  name: default
cluster: null
"

wget "https://github.com/blockcaptain/blockcaptain-eng/releases/latest/download/blkcapteng"
sudo mv blkcapteng /usr/local/bin
sudo chown root: /usr/local/bin/blkcapteng
sudo chmod 755 /usr/local/bin/blkcapteng

mkdir -p target/debian

# This will change when using upstream restic
wget "https://github.com/blockcaptain/restic/releases/latest/download/restic" -O target/debian/restic
