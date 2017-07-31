#!/bin/bash
cd /root/proxy
. ./env/bin/activate
export YT_DRIVER_CONFIG_PATH=/control/console_driver_config.yson
LOG=$1
shift 1;
exec ./proxy.py $@ 2>$LOG
