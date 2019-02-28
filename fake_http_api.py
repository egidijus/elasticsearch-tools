#!/bin/env python
import sys
from http.server import BaseHTTPRequestHandler,HTTPServer
import time
from datetime import datetime
import json

def time():
    timestamp = str(datetime.now())
    return timestamp


class test_server(BaseHTTPRequestHandler):
  
  def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type','text/html')
        self.end_headers()
        data = {}
        data["time"] = time()
        json_data = json.dumps(data)
        message = json_data
        self.wfile.write(bytes(message, "utf8"))
        return
 
def run():
    print('starting server...')
    server_address = ('127.0.0.1', 8081)
    httpd = HTTPServer(server_address, test_server)
    print('running server...', datetime.now())
    httpd.serve_forever()

run()