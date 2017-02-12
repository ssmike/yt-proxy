#!/bin/bash
cd /root/proxy
virtualenv env -p python2 --system-site-packages
. env/bin/activate
pip install -r /root/proxy/requirements.txt
#pip install -i https://pypi.yandex-team.ru/simple/ yandex-yt
