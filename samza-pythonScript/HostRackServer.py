import sys
import subprocess
import re
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
def readHostRack():
    path  = sys.argv[0]
    f = open(path, "r")
    hosts = {}
    for line in f:
        values = re.split(r" +", line)
        hosts[values[0]] = values[1:]
    hosts
class RequestHandler(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
    def do_GET(self):
        response = readHostRack()
        self._set_headers()
        self.wfile.write(json.dumps(response))

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        print ('post data from client:')
        print (post_data)

        response = {
            'status':'SUCCESS',
            'data':'server got your post data'
        }
        self._set_headers()
        self.wfile.write(json.dumps(response))

def run():
    port = 8880
    print('Listening on localhost:%s' % port)
    server = HTTPServer(('', port), RequestHandler)
    server.serve_forever()


run()

json.dumps(containers, separators=(',', ':'))
