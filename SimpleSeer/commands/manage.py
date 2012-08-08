from .base import Command

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
    

    
