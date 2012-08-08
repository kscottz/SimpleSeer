from .base import Command
import os
import os.path
import sys

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

    def run(self):
        print "This will destroy ALL DATA in database \"%s\", type YES to proceed:"
        if sys.stdin.readline() == "YES\n":
            os.system('echo "db.dropDatabase()" | mongo ' + self.options.database)
        else:
            print "reset cancelled"


@ManageCommand.simple()
def BuildCommand(self):
    "Rebuild CoffeeScript/brunch in SimpleSeer and the process"
    import SimpleSeer.template as sst
    cwd = os.path.realpath(os.getcwd())
    print "Updating " + cwd
    sst.SimpleSeerProjectTemplate("").post("", cwd, { "package": cwd.split("/")[-1] })
