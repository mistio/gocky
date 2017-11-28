#!/bin/bash

function disable_systemd {
    systemctl disable gocky
    rm -f /lib/systemd/system/gocky.service
}

function disable_update_rcd {
    update-rc.d -f gocky remove
    rm -f /etc/init.d/gocky
}

function disable_chkconfig {
    chkconfig --del gocky
    rm -f /etc/init.d/gocky
}

if [[ -f /etc/redhat-release ]]; then
    # RHEL-variant logic
    if [[ "$1" = "0" ]]; then
	# gocky is no longer installed, remove from init system
	which systemctl &>/dev/null
	if [[ $? -eq 0 ]]; then
	    disable_systemd
	else
	    # Assuming sysv
	    disable_chkconfig
	fi
    fi
elif [[ -f /etc/lsb-release ]]; then
    # Debian/Ubuntu logic
    if [[ "$1" != "upgrade" ]]; then
	# Remove/purge
	which systemctl &>/dev/null
	if [[ $? -eq 0 ]]; then
	    disable_systemd
	else
	    # Assuming sysv
	    disable_update_rcd
	fi
    fi
elif [[ -f /etc/os-release ]]; then
    source /etc/os-release
    if [[ $ID = "amzn" ]]; then
	# Amazon Linux logic
	if [[ "$1" = "0" ]]; then
	    # gocky is no longer installed, remove from init system
	    disable_chkconfig
	fi
    fi
fi
