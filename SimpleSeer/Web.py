from base import *
from Session import *


atexit.register(cherrypy.engine.exit)
class Web():
    """
    This is the abstract web interface to handle event callbacks for Seer
    all it does is basically fire up a webserver on port 53317 to allow you
    to start interacting with Seer via a web interface
    """
    config =  { 'global' :
                {
                'server.socket_port': 53317,
                'server.socket_host' : '0.0.0.0',
                'log.screen' : True,
                'tools.staticdir.on': True,
                'tools.staticdir.dir': os.getcwd() + "/public/",
                }
            }

    def __init__(self):
        cherrypy.tree.mount(WebInterface())
        cherrypy.config.update(self.config)
        cherrypy.engine.start()



    

class WebInterface(object):
    """
    This is where all the event call backs and data handling happen for the
    internal webserver for Seer
    """

    @cherrypy.expose
    def index(self):
        filename = "index.html"
        subdirectory = "public"
        f = urllib.urlopen(subdirectory + "/" + filename)
        s = f.read() # read the file
        f.close()
        return s

    @cherrypy.expose
    def inspection_add(self, **params):
        
        cherrypy.response.headers['Content-Type'] = 'application/json'
        
        #try:
        Inspection(
            name = params["name"],
            camera = params["camera"],
            method = params["method"],
            parameters = json.loads(params["parameters"])).save()
        #except Exception as e:
        #    return dict( error = e )
        
        return jsonencode(SimpleSeer.SimpleSeer().inspections)



    @cherrypy.expose
    def poll(self):
        text = "Wow, this is some fun stuff"
        return json.dumps(text)


import SimpleSeer
from Inspection import Inspection
