#!/usr/bin/env bash
#
# Prepares a virtual machine for running TFB
#
# Intentionally uses ~, $HOME, and $USER so that the 
# same script can work for VirtualBox (username vagrant)
# and Amazon (username ubuntu)

# Add everything passed in the first argument to our 
# local environment. This is a hack to let us use 
# environment variables defined on the host inside the 
# guest machine
while read -r line; do  
  export $line; 
done <<< "$1"

# Store any custom variables used at launch, in case someone forgets
# what this instance is (e.g. SSD or HDD, etc)
echo "$1" > ~/.tfb_launch_options

# Are we installing the server machine, the client machine, 
# the database machine, or all machines? 
# Valid values: 
#    - all      (we are setting up a development environment)
#    - database (we are setting up the database machine)
#    - client   (we are setting up the client machine for load generation)
#    - server   (we are setting up the machine that will host frameworks)
ROLE=${2:-all}

# Set a number of variables by either pulling them from 
# the existing environment or using the default values
# I'm renaming them to indicate that (in this script only)
# the values are provisioner agnostic
SERVER_IP=${TFB_AWS_APP_IP:-172.16.0.16}
CLIENT_IP=${TFB_AWS_LOAD_IP:-172.16.0.17}
DATABA_IP=${TFB_AWS_DB_IP:-172.16.0.18}
if [ "$ROLE" == "all" ]; then
  SERVER_IP=127.0.0.1
  CLIENT_IP=127.0.0.1
  DATABA_IP=127.0.0.1
fi

GH_REPO=${TFB_AWS_REPO_SLUG:-TechEmpower/FrameworkBenchmarks}
GH_BRANCH=${TFB_AWS_REPO_BRANCH:-master}

# A shell provisioner is called multiple times
if [ ! -e "~/.firstboot" ]; then

  # Setup hosts
  echo "Setting up convenience hosts entries"
  echo $DATABA_IP TFB-database | sudo tee --append /etc/hosts
  echo $CLIENT_IP TFB-client   | sudo tee --append /etc/hosts
  echo $SERVER_IP TFB-server   | sudo tee --append /etc/hosts

  # Update hostname to reflect our current role
  if [ "$ROLE" != "all" ]; then
    echo "Updating hostname"
    echo 127.0.0.1 `hostname` | sudo tee --append /etc/hosts
    myhost=TFB-${ROLE}
    echo $myhost | sudo tee --append /etc/hostname
    sudo hostname $myhost
    echo Updated /etc/hosts file to be:
    cat /etc/hosts
  fi

  # Workaround mitchellh/vagrant#289
  echo "grub-pc grub-pc/install_devices multiselect     /dev/sda" | sudo debconf-set-selections

  # Install prerequisite tools
  echo "Installing prerequisites"
  sudo apt-get update
  sudo apt-get install -y git


  # Make project available
  # If they synced it to /FwBm, just expose it at ~/FwBm
  # If they didn't sync, we need to clone it
  #if [ -d "/FrameworkBenchmarks" ]; then
    #ln -s /FrameworkBenchmarks $FWROOT
    #echo "Removing your current results folder to avoid interference"
    #rm -rf $FWROOT/installs $FWROOT/results

    # vboxfs does not support chown or chmod, which we need.
    # We therefore bind-mount a normal linux directory so we can
    # use these operations.
    #echo "Mounting over your installs folder"
    #mkdir -p /tmp/TFB_installs
    #mkdir -p /FrameworkBenchmarks/installs
    #sudo mount -o bind /tmp/TFB_installs $FWROOT/installs
  #else
    # If there is no synced folder, clone the project
    export FWROOT="/home/vagrant/FrameworkBenchmarks"
    echo `export FWROOT="/home/vagrant/FrameworkBenchmarks"` >> ~/.bashrc
    echo "Cloning project from $GH_REPO $GH_BRANCH"
    git config --global core.autocrlf input
    git clone -b ${GH_BRANCH} https://github.com/${GH_REPO}.git $FWROOT
    cd ~/FrameworkBenchmarks
    # Update the benchmark.cfg for vagrant
    cp ~/FrameworkBenchmarks/benchmark.cfg.example ~/FrameworkBenchmarks/benchmark.cfg
    sed -i s/techempower/vagrant/g ~/FrameworkBenchmarks/benchmark.cfg
    source ~/FrameworkBenchmarks/toolset/setup/linux/prerequisites.sh
  #fi

 # Everyone gets SSH access to localhost
 echo "Setting up SSH access to localhost"
 ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa
 cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
 chmod 600 ~/.ssh/authorized_keys

  # Enable remote SSH access if we are running production environment
  # Note : this is always copied from the local working copy using a
  #        file provisioner. While they exist in the git clone we just
  #        created (so we could use those), we want to let the user
  #        have the option of replacing the keys in their working copy
  #        and ensuring that only they can ssh into the machines
  if [ "$ROLE" == "server" ]; then
    # Ensure keys have proper permissions
    chmod 600 ~/.ssh/client ~/.ssh/database
  elif [ "$ROLE" != "all" ]; then
    # Ensure keys can be used to ssh in
    echo "Setting up SSH access for the TFB-server"
    mykey=~/.ssh/$ROLE.pub
    echo "Using key: "
    ssh-keygen -lv -f $mykey
    cat $mykey >> ~/.ssh/authorized_keys

    # Ensure keys have proper permissions
    chmod 600 ~/.ssh/client ~/.ssh/database
  fi

  # Setup a nice welcome message for our guest
  echo "Setting up welcome message"
  sudo rm -f /etc/update-motd.d/51-cloudguest
  sudo rm -f /etc/update-motd.d/98-cloudguest
  sudo mv ~/.custom_motd.sh /etc/update-motd.d/55-tfbwelcome
fi
