#!/usr/bin/env python2
from __future__ import print_function
import yt.yson as yson
import json
import yt.wrapper as yt
import sys
import os
import time
import uuid

PATH="//atom"
raw_config = open(os.environ['YT_DRIVER_CONFIG_PATH']).read()
yt.config = yson.loads(raw_config)

def wait_for_yt():
    toBreak = False
    while not toBreak:
        toBreak = True
        try:
            yt.get('/')
        except Exception as e:
            toBreak = False
            time.sleep(2)


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def handlemessage():
    raw_message = raw_input()
    eprint(raw_message)
    message = json.loads(raw_message)
    op = message["f"]
    try:
        if op == "wait-for-yt":
            wait_for_yt()

        elif op == "read":
            val = int(yt.get(PATH))
            message["value"] = val
        elif op == "write":
            yt.set(PATH, message["value"])
        message["type"] = "ok"
    except Exception as e:
        my_id = uuid.uuid4()
        eprint(my_id)
        eprint(e)
        if op == "write":
            message["type"] = "info"
        else:
            message["type"] = "fail"
        message["error"] = my_id
    print(json.dumps(message))
    sys.stdout.flush()
    if op == "terminate":
        sys.exit(0)


if __name__ == "__main__":
    while True:
        handlemessage()
