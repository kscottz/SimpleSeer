
def load_ipython_extension(ipython):
    from .Session import Session
    from .realtime import ChannelManager
    from . import models as M
    from SimpleCV import Display, Image, ImageSet, Color
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
            Image = Image,
            ImageSet = ImageSet,
            Color = Color,
            display=Display(displaytype="notebook"), 
            M=M,
            cm=ChannelManager(zmq.Context.instance())),
        interactive=True)
    ipython.prompt_manager.in_template="SimpleSeer:\\#> "
    ipython.prompt_manager.out_template="SimpleSeer:\\#: "
    print 'SimpleSeer ipython extension loaded ok'

def unload_ipython_extension(ipython):
    # If you want your extension to be unloadable, put that logic here.
    pass

