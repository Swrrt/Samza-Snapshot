import sys
import subprocess
import re
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
containers = {}
def readYARN():
# add time interval heres
    appattempt  = sys.argv[1]
    yarn_home = '/home/samza/cluster/yarn/'
    contents = ""
    try:
        contents = subprocess.check_output(['bash',yarn_home+'bin/yarn','container','-list', appattempt])
    except subprocess.CalledProcessError as e:
        contents
    containers = {}
    print(contents)
    for line in contents.decode('utf-8').splitlines():
        print(line)
        if(line.find('container')!=-1 and line.find('Total')==-1):
            values = re.split(r" +", line)
            containers[values[0]] = re.split(r"\t+",values[4])[1].split(":")[0]
    return(containers)
class RequestHandler(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
    def do_GET(self):
        response = readYARN()
        self._set_headers()
        self.wfile.write(bytes(json.dumps(response), 'UTF-8'))

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
        self.wfile.write(bytes(json.dumps(response),'UTF-8'))

def run():
    port = 8881
    print('Listening on localhost:%s' % port)
    server = HTTPServer(('', port), RequestHandler)
    server.serve_forever()


run()

json.dumps(containers, separators=(',', ':'))
