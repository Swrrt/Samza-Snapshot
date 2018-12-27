import sys
import subprocess
import re
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
def readYARN():
    appattempt  = sys.argv[0]
    yarn_home = '~/cluster/yarn/'
    contents = subprocess.check_output('./' + yarn_home + 'bin/yarn container -list ' + appattempt)
    containers = {}
    for line in contents:
        if(re.fullmatch(r"container.*")!= None):
            values = re.split(r" +", line)
            containers[values[0]] = values[4]
    containers
class RequestHandler(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
    def do_GET(self):
        response = readYARN()
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
    port = 8881
    print('Listening on localhost:%s' % port)
    server = HTTPServer(('', port), RequestHandler)
    server.serve_forever()


run()

json.dumps(containers, separators=(',', ':'))
