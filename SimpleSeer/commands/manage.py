from .base import Command
import os
import os.path
import sys
import pkg_resources
import subprocess
import time
from path import path

class ManageCommand(Command):
    "Simple management tasks that don't require SimpleSeer context"
    use_gevent = False
    remote_seer = False

    def configure(self, options):
        self.options = options
    
class CreateCommand(ManageCommand):
    "Create a new repo"
    
    def __init__(self, subparser):
        subparser.add_argument("projectname", help="Name of new project")
        
    def run(self):
        from paste.script import command as pscmd
        pscmd.run(["create", "-t", "simpleseer", self.options.projectname])


class ResetCommand(ManageCommand):
    "Clear out the database"
    
    def __init__(self, subparser):
        subparser.add_argument("database", help="Name of database", default="default", nargs='?')

    #TODO, this should probably be moved to a pymongo command and include a supervisor restart all
    def run(self):
        print "This will destroy ALL DATA in database \"%s\", type YES to proceed:"
        if sys.stdin.readline() == "YES\n":
            os.system('echo "db.dropDatabase()" | mongo ' + self.options.database)
        else:
            print "reset cancelled"

class DeployCommand(ManageCommand):
    "Deploy an instance"
    def __init__(self, subparser):
        subparser.add_argument("directory", help="Target", default = os.path.realpath(os.getcwd()), nargs = '?')

    def run(self):
        link = "/etc/simpleseer"
        if os.path.exists(link):
            os.remove(link)
            
        print "Linking %s to %s" % (self.options.directory, link)
        os.symlink(self.options.directory, link)
        print "Restarting jobs in supervisord"
        subprocess.check_output(['supervisorctl', 'restart all'])



@ManageCommand.simple()
def WatchCommand(ManageCommand):
    cwd = os.path.realpath(os.getcwd())
    package = cwd.split("/")[-1]

    src_brunch = path(pkg_resources.resource_filename(
        'SimpleSeer', 'static'))
    tgt_brunch = path(cwd) / package / 'brunch_src'
    
    #i'm not putting this in pip, since this isn't necessary in production
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    
    seer_event_handler = FileSystemEventHandler()
    seer_event_handler.eventqueue = []
    def rebuild(event):
        seer_event_handler.eventqueue.append(event)
    
    seer_event_handler.on_any_event = rebuild
    
    seer_observer = Observer()
    seer_observer.schedule(seer_event_handler, path=src_brunch, recursive=True)
    
    local_event_handler = FileSystemEventHandler()
    local_event_handler.eventqueue = []
    
    def build_local(event):
        local_event_handler.eventqueue.append(event)
        
    local_event_handler.on_any_event = build_local
    
    local_observer = Observer()
    local_observer.schedule(local_event_handler, path=tgt_brunch, recursive=True)
    
    seer_observer.start()
    local_observer.start()
    
    while True:
        if len(seer_event_handler.eventqueue):
            time.sleep(0.2)
            BuildCommand("").run()
            time.sleep(0.1)
            seer_event_handler.eventqueue = []
            local_event_handler.eventqueue = []
        
        if len(local_event_handler.eventqueue):
            time.sleep(0.2)
            with tgt_brunch:
                print "Updating " + cwd
                print subprocess.check_output(['brunch', 'build'])
            local_event_handler.eventqueue = []
                
        time.sleep(0.5)



@ManageCommand.simple()
def BuildCommand(self):
    "Rebuild CoffeeScript/brunch in SimpleSeer and the process"
    import SimpleSeer.template as sst
    cwd = os.path.realpath(os.getcwd())
    print "Updating " + cwd
    sst.SimpleSeerProjectTemplate("").post("", cwd, { "package": cwd.split("/")[-1] })
