#!/usr/bin/bash
#
# Copyright 2022 Authors of Global Resource Service.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# This script is used to quickly install configure Redis Server 
# on Ubuntun20.04/18.04/16.04 and MacOS(Darwin 20.6.0)
#
#    Running on Ubuntu 20.04: ./hack/redis_install.sh
#                              Redis 7.0.0 is installed as default
#    Running on Ubuntu 18.04:  /bin/bash ./hack/redis_install.sh
#                              Redis 7.0.0 is installed as default
#    Running on Ubuntu 16.04:  /bin/bash ./hack/redis_install.sh
#                              Redis 7.0.0 is installed as default
#
#    Running on MacOS:  /bin/bash ./hack/redis_install.sh
#                       Redis 7.0.0 is installed as default
#
# Reference: 
#    For Ubuntu: https://redis.io/docs/getting-started/installation/install-redis-on-linux/
#                https://www.linode.com/docs/guides/install-redis-ubuntu/
#
#    For MacOS:  https://redis.io/docs/getting-started/installation/install-redis-on-mac-os/
#    
#
export PATH=$PATH

REDIS_DEFAULT_PORT=${REDIS_DEFAULT_PORT:-6379}
REDIS_NEW_PORT=${REDIS_NEW_PORT:-7379}
REDIS_CONF_UBUNTU=${REDIS_CONF_UBUNTU:-"/etc/redis/redis.conf"}


if [ `uname -s` == "Linux" ]; then
  LINUX_OS=`uname -v |awk -F'-' '{print $2}' |awk '{print $1}'`

  if [ "$LINUX_OS" == "Ubuntu" ]; then
    UBUNTU_VERSION_ID=`grep VERSION_ID /etc/os-release |awk -F'"' '{print $2}'`

    echo "1. Install Redis on Ubuntu ......"
    REDIS_GPG_FILE=/usr/share/keyrings/redis-archive-keyring.gpg
    if [ -f $REDIS_GPG_FILE ]; then
      sudo rm -f $REDIS_GPG_FILE
    fi 
    curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o $REDIS_GPG_FILE

    echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list

    sudo apt-get update

    if [ "$UBUNTU_VERSION_ID" == "20.04" ]; then
      REDIS_VERSION="6:7.0.0-1rl1~focal1"
    elif [ "$UBUNTU_VERSION_ID" == "18.04" ]; then
      REDIS_VERSION="6:7.0.0-1rl1~bionic1"
    elif [ "$UBUNTU_VERSION_ID" == "16.04" ]; then
      REDIS_VERSION="6:7.0.0-1rl1~xenial1"
    else
      echo "The Ubuntu $UBUNTU_VERSION_ID is not currently supported and exit"
      exit 1
    fi

    echo "Purge existing version of Redis ......"
    sudo apt-get purge redis -y
    sudo apt-get purge redis-server -y
    sudo apt-get purge redis-tools -y

    echo "Install Redis 7.0.0 ......"
    sudo apt-get install redis-tools=$REDIS_VERSION
    sudo apt-get install redis-server=$REDIS_VERSION
    sudo apt-get install redis=$REDIS_VERSION
    echo "End to install on Ubuntu ......"

    echo ""
    echo "2. Enable and Run Redis ......"
    echo "==============================="
    sudo ls -alg $REDIS_CONF_UBUNTU

    sudo sed -i -e "s/^supervised auto$/supervised systemd/g" $REDIS_CONF_UBUNTU
    sudo egrep -v "(^#|^$)" $REDIS_CONF_UBUNTU |grep "supervised "

    sudo sed -i -e "s/^appendonly no$/appendonly yes/g" $REDIS_CONF_UBUNTU
    sudo egrep -v "(^#|^$)" $REDIS_CONF_UBUNTU |egrep "(appendonly |appendfsync )"
    
    # === Enable Redis server remote support
    sudo sed -i -e "s/^bind 127.0.0.1 -::1$/bind 0.0.0.0/g" $REDIS_CONF_UBUNTU
    sudo egrep -v "(^#|^$)" $REDIS_CONF_UBUNTU |egrep "(bind 0.0.0.0)"

    sudo sed -i -e "s/^protected-mode yes$/protected-mode no/g" $REDIS_CONF_UBUNTU
    sudo egrep -v "(^#|^$)" $REDIS_CONF_UBUNTU |egrep "(protected-mode no)"

    sudo sed -i -e "s/^port $REDIS_DEFAULT_PORT$/port $REDIS_NEW_PORT/g" $REDIS_CONF_UBUNTU
    sudo egrep -v "(^#|^$)" $REDIS_CONF_UBUNTU |egrep "(port $REDIS_NEW_PORT)"
    # === Enable Redis server remote support

    #
    # Note: do not forget to open redis port 7379 in AWS Ubuntu OS level or GCE level
    #
    # For example: Open port 7379 with ufw on AWS 
    # $ sudo ufw status
    # $ sudo ufw allow 7379/tcp
    # $ sudo ufw status
    #
    # if ufw status is inactive, please enable ufw
    # $ sudo ufw enable
    # $ sudo ufw status
    #

    # Restart redis-server as a service
    sudo ls -al /lib/systemd/system/ |grep redis

    sudo systemctl restart redis-server.service
    sudo systemctl status redis-server.service

    # Check whether network port 7379 is listened
    sudo netstat -nlpt | grep $REDIS_NEW_PORT
  else
    echo ""
    echo "This Linux OS ($LinuxOS) is currently not supported and exit"
    exit 1
  fi
elif [ `uname -s` == "Darwin" ]; then
  echo "1. Install and configure Redis on MacOS ......"
  brew --version

  echo ""
  echo "Uninstall existing version of Redis ......"

  brew uninstall redis

  echo "Remove three files ......"
  REDIS_SENTINEL=/usr/local/etc/redis-sentinel.conf
  REDIS_CONF_MacOS=/usr/local/etc/redis.conf
  REDIS_CONF_E=/usr/local/etc/redis.conf-e

  echo "Remove $REDIS_SENTINEL ......"
  rm -rf $REDIS_SENTINEL if [ -f $REDIS_SENTINEL ]

  echo "Remove $REDIS_CONF_MacOS ......"
  rm -rf $REDIS_CONF_MacOS if [ -f $REDIS_CONF_MacOS ]

  echo "Remove $REDIS_CONF_E ......"
  rm -rf $REDIS_CONF_E if [ -f $REDIS_CONF_E ]

  echo "It is done to remove three files ......"
  echo ""

  echo "Install Redis 7.0 ......"
  brew install redis@7.0
  brew services start redis
  brew services info redis --json

  echo "End to install Redis on MacOS ......"

  echo ""
  echo "2. Enable and Run Redis ......"
  echo "==============================="
  sed -i -e "s/^# supervised auto$/supervised systemd/g" $REDIS_CONF_MacOS
  egrep -v "(^#|^$)" $REDIS_CONF_MacOS |grep "supervised "

  #
  #Configure Redis Persistence using Append Only File (AOF)
  #
  sed -i -e "s/^appendonly no$/appendonly yes/g" $REDIS_CONF_MacOS
  egrep -v "(^#|^$)" $REDIS_CONF_MacOS |egrep "(appendonly |appendfsync )"

  brew services stop redis
  sleep 2
  brew services start redis
  brew services info redis --json
else
  echo ""
  echo "Unknown OS and exit"
  exit 1
fi

echo ""
echo "Sleeping for 5 seconds after Redis installation ......"
sleep 5

echo ""
echo "3. Simply Test Redis ......"
echo "==============================="
which redis-cli

echo "3.1) Test ping ......"
redis-cli -p $REDIS_NEW_PORT ping 

echo ""
echo "3.2) Test write key and value ......"
redis-cli -p $REDIS_NEW_PORT << EOF
SET server:name "fido"
GET server:name
EOF

echo ""
echo "3.3) Test write queue ......"
redis-cli -p $REDIS_NEW_PORT << EOF
lpush demos redis-macOS-demo
rpop demos
EOF

echo ""
echo "Sleep 5 seconds after Redis tests ..."
sleep 5

# Redis Persistence Options:
#
# 1.Redis Database File (RDB) persistence takes snapshots of the database at intervals corresponding to the save directives in the redis.conf file. The redis.conf file contains three default intervals. RDB persistence generates a compact file for data recovery. However, any writes since the last snapshot is lost.

# 2. Append Only File (AOF) persistence appends every write operation to a log. Redis replays these transactions at startup to restore the database state. You can configure AOF persistence in the redis.conf file with the appendonly and appendfsync directives. This method is more durable and results in less data loss. Redis frequently rewrites the file so it is more concise, but AOF persistence results in larger files, and it is typically slower than the RDB approach

echo ""
echo "************************************************************"
echo "*                                                          *"
echo "* You are successful to install and configure Redis Server *"
echo "*                                                          *"
echo "************************************************************"

exit 0
