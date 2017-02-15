#!/bin/bash
cd /root/proxy
. ./env/bin/activate
export YT_DRIVER_CONFIG_PATH=/control/console_driver_config.yson
exec ./proxy.py $@ 2>/root/proxy-$1.log
