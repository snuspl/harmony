#!/bin/bash
# Copyright (C) 2016 Seoul National University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Install packages and leave a log in /tmp/install_reef_log file to track the progress.
LOG_FILE=/tmp/install_reef.log

function install_essential {
  # SSH, rsync, Git, Maven
  sudo apt-get install -y ssh rsync git maven

  # SSH key
  ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
  
  echo "Essential packages installed" >> $LOG_FILE
}

function install_protobuf {
  #Install prerequisite for protobuf
  sudo apt-get install -y automake autoconf g++ make
  
  cd /tmp
  wget https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.bz2
  tar xjf protobuf-2.5.0.tar.bz2
  rm protobuf-2.5.0.tar.bz2
  cd /tmp/protobuf-2.5.0
  ./configure
  sudo make
  sudo make install
  sudo ldconfig
  
  echo "Protobuf installed" >> $LOG_FILE
}

# Let's go with HotSpot, as OpenJDK has an issue (http://stackoverflow.com/questions/8375423/missing-artifact-com-suntoolsjar). Note that keyboard interrupt is required while installation (Choose <OK>, then <YES>).
function install_java {
  sudo add-apt-repository ppa:webupd8team/java
  sudo apt-get update
  sudo apt-get install -y oracle-java8-installer
  export JAVA_HOME=/usr/lib/jvm/java-8-oracle/
  echo "export JAVA_HOME=$JAVA_HOME" >> ~/.profile
  
  # Register the variable to SSH Environment so that they can be accessed even via SSH connection
  echo "JAVA_HOME=$JAVA_HOME" >> ~/.ssh/environment

  echo "Java installed" >> $LOG_FILE
}

# Install hadoop on $HOME/hadoop
function install_hadoop {
  cd /tmp
  wget http://mirror.navercorp.com/apache/hadoop/common/hadoop-2.7.2/hadoop-2.7.2.tar.gz
  tar -xzvf hadoop-2.7.2.tar.gz
  mv hadoop-2.7.2 ~/hadoop

  export HADOOP_HOME=$HOME/hadoop
  export OLD_PATH=$PATH
  
  # Register variables to profile
  echo "export HADOOP_HOME=$HADOOP_HOME" >> ~/.profile
  echo "export YARN_HOME=\$HADOOP_HOME" >> ~/.profile
  echo "export YARN_CONF_DIR=\$HADOOP_HOME/etc/hadoop" >> ~/.profile
  echo "export PATH=\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:$OLD_PATH" >> ~/.profile
  
  # Register variables to SSH Environment so that they can be accessed even via SSH connection
  echo "HADOOP_HOME=$HOME/hadoop" >> ~/.ssh/environment
  echo "YARN_HOME=$HADOOP_HOME" >> ~/.ssh/environment
  echo "YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop" >> ~/.ssh/environment
  
  echo "Hadoop installed" >> $LOG_FILE
}

function install_reef {
  install_protobuf # Protobuf is needed only when building REEF.
  cd ~
  git clone https://github.com/apache/reef
  cd reef
  mvn clean -TC1 install -DskipTests -Dcheckstyle.skip=true -Dfindbugs.skip=true
  
  echo "REEF installed" >> $LOG_FILE
}

# Normally we don't have to build cay in all machines.
function install_cay {
  # Install Fortran (which cay depends on)
  sudo apt-get install -y libgfortran3

  cd ~
  git clone https://github.com/cmssnu/cay # Username/password is required
  cd cay
  mvn clean -TC1 install -DskipTests -Dcheckstyle.skip=true -Dfindbugs.skip=true
  
  echo "Cay installed" >> $LOG_FILE
}

# Hadoop RPCs may fail if IPv6 is enabled.
# It's recommended to comment IPv6 addresses in /etc/hosts
function disable_ipv6 {
  sudo bash -c 'echo "net.ipv6.conf.all.disable_ipv6 = 1" >> /etc/sysctl.conf'
  sudo bash -c 'echo "net.ipv6.conf.default.disable_ipv6 = 1" >> /etc/sysctl.conf'
  sudo bash -c 'echo "net.ipv6.conf.lo.disable_ipv6 = 1" >> /etc/sysctl.conf'
  sudo sysctl -p
  echo "IPv6 disabled" >> $LOG_FILE
}

# Start installation
export LC_ALL="en_US.UTF-8"
echo "Install start: $(date)" > $LOG_FILE

install_essential
install_java
install_hadoop

# REEF and Cay can be built optionally.
# Comment out this line only when you want to build [REEF|cay] on your machine.
#install_reef 
#install_cay

# Wrapping up
disable_ipv6
sudo ufw disable # Disable firewall which also can block Hadoop RPCs
sudo /etc/init.d/ssh restart
source ~/.profile

echo "Done: $(date)" >> $LOG_FILE
