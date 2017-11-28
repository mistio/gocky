#!/bin/bash

BIN_DIR=/usr/bin
DATA_DIR=/var/lib/gocky
LOG_DIR=/var/log/gocky
SCRIPT_DIR=/usr/lib/gocky/scripts
LOGROTATE_DIR=/etc/logrotate.d

function install_init {
    cp -f $SCRIPT_DIR/init.sh /etc/init.d/gocky
    chmod +x /etc/init.d/gocky
}

function install_systemd {
    cp -f $SCRIPT_DIR/gocky.service /lib/systemd/system/gocky.service
    systemctl enable gocky
}

function install_update_rcd {
    update-rc.d gocky defaults
}

function install_chkconfig {
    chkconfig --add gocky
}

id gocky &>/dev/null
if [[ $? -ne 0 ]]; then
    useradd --system -U -M gocky -s /bin/false -d $DATA_DIR
fi

chown -R -L gocky:gocky $DATA_DIR
chown -R -L gocky:gocky $LOG_DIR

# Remove legacy symlink, if it exists
if [[ -L /etc/init.d/gocky ]]; then
    rm -f /etc/init.d/gocky
fi

# Distribution-specific logic
if [[ -f /etc/redhat-release ]]; then
    # RHEL-variant logic
    which systemctl &>/dev/null
    if [[ $? -eq 0 ]]; then
	install_systemd
    else
	# Assuming sysv
	install_init
	install_chkconfig
    fi
elif [[ -f /etc/debian_version ]]; then
    # Debian/Ubuntu logic
    which systemctl &>/dev/null
    if [[ $? -eq 0 ]]; then
	install_systemd
    else
	# Assuming sysv
	install_init
	install_update_rcd
    fi
elif [[ -f /etc/os-release ]]; then
    source /etc/os-release
    if [[ $ID = "amzn" ]]; then
	# Amazon Linux logic
	install_init
	install_chkconfig
    fi
fi
