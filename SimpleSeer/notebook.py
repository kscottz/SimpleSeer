

def load_ipython_extension(ipython):
    
    from .Session import Session
    from SimpleCV import Display, Image, ImageSet, Color
    from .realtime import ChannelManager
    from . import models as M
    import zmq
    
    s = Session("simpleseer.cfg")
    ipython.push(
        dict(
            Frame = M.Frame,
            Result = M.Result,
            OLAP = M.OLAP,
            FrameSet = M.FrameSet,
            Inspection = M.Inspection,
            Measurement = M.Measurement,
            M=M,
            Image = Image,
            ImageSet = ImageSet,
            Color = Color,
            display=Display(displaytype="notebook"), 
            cm=ChannelManager(zmq.Context.instance())),
        interactive=True)
    print 'SimpleSeer ipython extension loaded ok'

def unload_ipython_extension(ipython):
    # If you want your extension to be unloadable, put that logic here.
    pass

