# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

# User specific aliases and functions


export PYTHON_HOME=/opt/mount1/anaconda2

export GIT_HOME=/home/${USER}/AzureCodes

export PATH=$PYTHON_HOME/bin:$PATH

