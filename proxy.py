#!/usr/bin/env python2
from flask import Flask
from flask_sockets import Sockets
import yt.yson as yson
import json
import yt.wrapper as yt
import sys
import os
import time

app = Flask(__name__)
sockets = Sockets(app)

PATH="//atom"
raw_config = open(os.environ['YT_DRIVER_CONFIG_PATH']).read()
yt.config = yson.loads(raw_config)


@sockets.route('/')
def socket_handler(ws):
    while not ws.closed:
        raw_message = ws.receive()
        print(raw_message)
        if raw_message is None:
            print("shutting down proxy")
            sys.exit(0)
        message = json.loads(raw_message)
        try:
            if ("wait-for-yt" in message) and message["wait-for-yt"]:
                toBreak = False
                while not toBreak:
                    toBreak = True
                    try:
                        yt.get('/')
                    except Exception as e:
                        toBreak = False
                        time.sleep(2)
            elif message["f"] == "read":
                val = int(yt.get(PATH))
                message["value"] = val
            elif message["f"] == "write":
                yt.set(PATH, message["value"])
            message["type"] = "ok"
        except Exception as e:
            print(e)
            message["type"] = "fail"
            message["error"] = "yt-failure"
        ws.send(json.dumps(message))


if __name__ == "__main__":
    #forking
    if os.fork() != 0:
        sys.exit(0)
    port = 5000
    if len(sys.argv) >= 2:
        port = int(sys.argv[1])
    from gevent import pywsgi
    from geventwebsocket.handler import WebSocketHandler
    server = pywsgi.WSGIServer(('', port), app, handler_class=WebSocketHandler)
    server.serve_forever()

