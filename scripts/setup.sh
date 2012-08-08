#!/bin/sh
#This is a setup tool to help automate the installation of SimpleSeer
#Please run this from the top level seer directory
#so for instance if you are running in /tmp/SimpleSeer than
# cd /tmp/SimpleSeer
# source scripts/setup.sh
#
#

seer_manual_install () {
  reset
  echo "[Manual-Installing Seer]"
  read -p "Install System Requirements? (y/N) " -n 1 -r
  if [[ $REPLY =~ ^[Yy]$ ]]
  then
      install_requirements
  fi
  read -p "Install PIP Requirements? (y/N) " -n 1 -r
  if [[ $REPLY =~ ^[Yy]$ ]]
  then
      install_pip_requirements
  fi
  read -p "Setup System Environment? (y/N) " -n 1 -r
  if [[ $REPLY =~ ^[Yy]$ ]]
  then
      setup_environment
  fi
  read -p "Setup MongoDB? (y/N) " -n 1 -r
  if [[ $REPLY =~ ^[Yy]$ ]]
  then
      mongo_install
  fi  
  read -p "Setup SuperVisor? (y/N) " -n 1 -r
  if [[ $REPLY =~ ^[Yy]$ ]]
  then
      setup_supervisor
  fi
  
  echo "Everything should now be installed"

}


seer_auto_install () {
  reset
  echo "[Auto-Installing Seer]"
  install_requirements
  install_pip_requirements
  setup_environment
  mongo_install
  setup_supervisor
  echo "Everything should now be installed"
}

install_requirements() {
  echo "installing required system libraries"
  sudo apt-get install python-dev python-setuptools python-pip libzmq-dev nodejs npm build-essential python-gevent libevent-dev supervisor ipython-notebook swig libvpx-dev subversion
  echo "installing brunch"
  sudo npm install -g brunch
}

install_pip_requirements() {
  echo "installing PIP requirements"
  sudo pip install -r pip.requirements
}

setup_environment() {
  echo "linking for development"
  sudo python setup.py develop
  echo "setting up environment"
  sudo ln -s `pwd`/etc/supervisor.conf /etc/supervisor/conf.d/supervisor.conf
  echo "make sure to symlink your project to /etc/simpleseer or run simpleseer deploy!"
}

setup_supervisor() {
  echo "stopping supervisord"
  sudo supervisorctl stop all
  sudo killall supervisord
  echo "starting supervisord"
  sudo supervisord
}


seer_remove () {
  reset
  echo "[Removing Seer from system]"
  echo "stopping supervisord"
  sudo supervisorctl stop all
  sudo killall supervisord
  echo "cleaning up old files..."
  mongo_uninstall
  sudo pip uninstall simpleseer
  sudo rm /etc/simpleseer
  sudo rm -f /etc/supervisor/conf.d/supervisor.conf
  echo "...done deleting seer from system"
  echo ""
}

mongo_install () {
  echo "[Installing Mongo]"
  echo ""
  echo "setting up directories"
  mkdir -p /tmp/mongo
  if [ ! -f /tmp/mongodb-linux-x86_64-2.2.0-rc0.tgz ];
  then
    echo "downloading..."
    wget http://fastdl.mongodb.org/linux/mongodb-linux-x86_64-2.2.0-rc0.tgz -O /tmp/mongodb-linux-x86_64-2.2.0-rc0.tgz
  fi  
  sudo tar zxvf /tmp/mongodb-linux-x86_64-2.2.0-rc0.tgz -C /tmp/mongo
  echo "copying files...."
  sudo cp /tmp/mongo/mongodb-linux-x86_64-2.2.0-rc0/bin/* /usr/local/bin/
  sudo mkdir -p /var/lib/mongodb
  sudo mkdir -p /var/log/mongodb
  sudo cp `pwd`/etc/mongodb.conf /etc/
  echo "...done installing mongo"
  echo ""
}

mongo_uninstall () {
  echo "[Removing Mongo]"
  sudo killall mongod
  sudo apt-get -f -y --force-yes remove mongodb mongodb-server
  sudo rm -f /usr/local/bin/bsondump
  sudo rm -f /usr/local/bin/mongod
  sudo rm -f /usr/local/bin/mongoexport
  sudo rm -f /usr/local/bin/mongoimport
  sudo rm -f /usr/local/bin/mongoperf
  sudo rm -f /usr/local/bin/mongos
  sudo rm -f /usr/local/bin/mongotop
  sudo rm -f /usr/local/bin/mongo
  sudo rm -f /usr/local/bin/mongodump
  sudo rm -f /usr/local/bin/mongofiles
  sudo rm -f /usr/local/bin/mongooplog
  sudo rm -f /usr/local/bin/mongorestore
  sudo rm -f /usr/local/bin/mongostat
  sudo rm -f /usr/local/bin/perftest
  sudo rm -rf /var/lib/mongodb
  sudo rm -rf /var/log/mongodb
  echo "...done removing mongo"
}



# The main script launch point
reset
while :
do
    case "$1" in
      -i | --install)
        echo "install command line"
        seer_auto_install
        return
        ;;
      -r | --remove)
        echo "remove seer command line"
        seer_remove
        return
        ;;
      -s | --start)
        echo "start seer command line"
        start_seer
        return
        ;;        
    esac
    break
done


while true; do
    echo "-------------------------------------------------------------------------"
    echo "SimpleSeer setup tool"
    echo "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    echo "*run this from the main SimpleSeer directory with:"
    echo "source scripts/setup.sh"
    echo
    echo "-------------------------------------------------------------------------"
    echo "[a]uto install"
    echo "[m]anual install"
    echo "[r]emove from system"
    echo "e[x]it"
    echo
    read -p "which option:" choice
    case $choice in
        [a]* ) seer_auto_install;;
        [m]* ) seer_manual_install;;
        [r]* ) seer_remove;;
        [x]* ) echo "exiting...";break;;
        * ) echo "Please choose an option.";;
    esac
done
