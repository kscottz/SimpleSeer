import time
import gevent
from .base import Command

class CoreStatesCommand(Command):
    'Run the core server / state machine'
    use_gevent = True
    remote_seer = False

    def __init__(self, subparser):
        subparser.add_argument('program')
        subparser.add_argument('--disable-pyro', action='store_true')

    def run(self):
        from SimpleSeer.states import Core
        import Pyro4

        core = Core(self.session)
        found_statemachine = False
        with open(self.options.program) as fp:
            exec fp in dict(core=core)
            found_statemachine = True
        
        if not found_statemachine:
            raise Exception("State machine " + self.options.program + " not found!")
            

        core.start_socket_communication()

        if not self.options.disable_pyro:
            gevent.spawn_link_exception(core.run)
            Pyro4.Daemon.serveSimple(
                { core: "sightmachine.seer" },
                ns=True)
        else:
            core.run()

class CoreCommand(CoreStatesCommand):
    'Run the core server'

    def __init__(self, subparser):
        subparser.add_argument('--disable-pyro', action='store_true')

    def run(self):
        self.options.program = self.session.statemachine or 'states.py'
        super(CoreCommand, self).run()

@Command.simple(use_gevent=False, remote_seer=True)
def ControlsCommand(self):
    'Run a control event server'
    from SimpleSeer.Controls import Controls
    
    if self.session.arduino:
       Controls(self.session).run()

@Command.simple(use_gevent=False, remote_seer=False)
def PerfTestCommand(self):
    'Run the core performance test'
    from SimpleSeer.SimpleSeer import SimpleSeer
    from SimpleSeer import models as M

    self.session.auto_start = False
    self.session.poll_interval = 0
    seer = SimpleSeer()
    seer.run()

@Command.simple(use_gevent=True, remote_seer=False)
def OlapCommand(self):
    from SimpleSeer.OLAPUtils import ScheduledOLAP, RealtimeOLAP
    from SimpleSeer.models.Inspection import Inspection, Measurement

    Inspection.register_plugins('seer.plugins.inspection')
    Measurement.register_plugins('seer.plugins.measurement')

    so = ScheduledOLAP()
    gevent.spawn_link_exception(so.runSked)
    
    ro = RealtimeOLAP()
    ro.monitorRealtime()
    
    
@Command.simple(use_gevent=True, remote_seer=True)
def WebCommand(self):
    'Run the web server'
    from SimpleSeer.Web import WebServer, make_app
    from SimpleSeer import models as M
    from pymongo import Connection, DESCENDING, ASCENDING
    from SimpleSeer.models.Inspection import Inspection, Measurement

    # Plugins must be registered for queries
    Inspection.register_plugins('seer.plugins.inspection')
    Measurement.register_plugins('seer.plugins.measurement')

    # Ensure indexes created for filterable fields
    dbName = self.session.database
    if not dbName:
        dbName = 'default'
    db = Connection()[dbName]
    for f in self.session.ui_filters:
        db.frame.ensure_index([(f['filter_name'], ASCENDING), (f['filter_name'], DESCENDING)])
    
    
    web = WebServer(make_app())
    web.run_gevent_server()

@Command.simple(use_gevent=True, remote_seer=True)
def BrokerCommand(self):
    'Run the message broker'
    from SimpleSeer.broker import PubSubBroker
    from SimpleSeer import models as M
    psb = PubSubBroker(self.session.pub_uri, self.session.sub_uri)
    psb.start()
    psb.join()

@Command.simple(use_gevent=False, remote_seer=True)
def ScrubCommand(self):
    'Run the frame scrubber'
    from SimpleSeer import models as M
    retention = self.session.retention
    if not retention:
        self.log.info('No retention policy set, skipping cleanup')
        return
    while retention['interval']:
        q_csr = M.Frame.objects(imgfile__ne = None)
        q_csr = q_csr.order_by('-capturetime')
        q_csr = q_csr.skip(retention['maxframes'])
        for f in q_csr:
            f.imgfile.delete()
            f.imgfile = None
            f.save(False)
        self.log.info('Purged %d frame files', q_csr.count())
        time.sleep(retention["interval"])

@Command.simple(use_gevent=False, remote_seer=True)
def ShellCommand(self):
    'Run the ipython shell'
    import subprocess
    
    subprocess.call(["ipython", 
            '--ext', 'SimpleSeer.ipython', '--pylab'], stderr=subprocess.STDOUT)

@Command.simple(use_gevent=True, remote_seer=False)
def NotebookCommand(self):
    'Run the ipython notebook server'
    import subprocess
    subprocess.call(["ipython", "notebook",
            '--port', '5050',
            '--ext', 'SimpleSeer.ipython', '--pylab', 'inline'], stderr=subprocess.STDOUT)

