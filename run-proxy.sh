#!/bin/bash
cd /root/proxy
. ./env/bin/activate
export YT_DRIVER_CONFIG_PATH=/control/console_driver_config.yson
exec ./proxy.py $@ >/root/proxy-$1.log 2>&1
