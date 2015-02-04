#!/bin/bash -xue

function install_deb(){
    sudo aptitude install -y python-dev libffi-dev python-pip    
}

function install_centos(){
    sudo yum -y install python-devel libffi-devel
    # pip is not in the standard repo
    sudo yum -y install epel-release
    sudo yum -y install python-pip

    # This gets installed automatically in debian because recommended packages
    # Doing that manually in centos.
    sudo yum -y install gcc

}


function is_centos(){
    [[ -f /etc/centos-release ]]
}

function is_deb(){
    [[ -f /etc/debian_version ]]
}

function pip_install(){
    sudo pip install tox
}

function install(){
    if is_deb; 
        then 
            install_deb;
    elif is_centos;
        then
            install_centos;
    else
        echo "Unknown distribution" ;
        exit 1;
    fi
    pip_install;
}

install;
