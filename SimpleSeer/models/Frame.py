from cStringIO import StringIO

import mongoengine

from SimpleSeer.base import Image, pil, pygame
from SimpleSeer import util

from formencode import validators as fev
from formencode import schema as fes
import formencode as fe

from .base import SimpleDoc
from .FrameFeature import FrameFeature
from .. import realtime


class FrameSchema(fes.Schema):	
    camera = fev.UnicodeString(not_empty=True)
	#TODO, make this feasible as a formencode schema for upload




class Frame(SimpleDoc, mongoengine.Document):
    """
        Frame Objects are a mongo-friendly wrapper for SimpleCV image objects,
        containing additional properties for the originating camera and time of capture.

        Note that Frame.image property must be used as a getter-setter.

        >>> f = SimpleSeer.capture()[0]  #get a frame from the SimpleSeer module
        >>> f.image.dl().line((0,0),(100,100))
        >>> f.save()
        >>> 
    """
    capturetime = mongoengine.DateTimeField()
    camera = mongoengine.StringField()
    features = mongoengine.ListField(mongoengine.EmbeddedDocumentField(FrameFeature))
    #features     
    
    height = mongoengine.IntField(default = 0)
    width = mongoengine.IntField(default = 0)
    imgfile = mongoengine.FileField()
    layerfile = mongoengine.FileField()
    _imgcache = ''
    results = [] #cache for result objects when frame is unsaved

    meta = {
        'indexes': ["capturetime", ('camera', '-capturetime')]
    }


    @property
    def image(self):
        if self._imgcache != '':
            return self._imgcache

        self.imgfile.get().seek(0,0) #hackity hack, make sure the FP is at 0
        if self.imgfile != None:
            try:
                self._imgcache = Image(pil.open(StringIO(self.imgfile.read())))
            except (IOError, TypeError): # pragma no cover
                self._imgcache = None
        else: # pragma no cover
            self._imgcache = None


        if self.layerfile:
            self.layerfile.get().seek(0,0)
            self._imgcache.dl()._mSurface = pygame.image.fromstring(self.layerfile.read(), self._imgcache.size(), "RGBA")

        return self._imgcache

    @image.setter
    def image(self, value):
        self.width, self.height = value.size()
        self._imgcache = value
       
    def __repr__(self): # pragma no cover
       return "<SimpleSeer Frame Object %d,%d captured with '%s' at %s>" % (
            self.width, self.height, self.camera, self.capturetime.ctime()) 
        
    def save(self, *args, **kwargs):
        #TODO: sometimes we want a frame with no image data, basically at this
        #point we're trusting that if that were the case we won't call .image
        realtime.ChannelManager().publish('frame.', self)

        if self._imgcache != '':
            s = StringIO()
            img = self._imgcache
            img.getPIL().save(s, "jpeg", quality = 100)
            self.imgfile.delete()
            self.imgfile.put(s.getvalue(), content_type = "image/jpg")
          
            if len(img._mLayers):
                if len(img._mLayers) > 1:
                    mergedlayer = DrawingLayer(img.size())
                    for layer in img._mLayers[::-1]:
                        layer.renderToOtherLayer(mergedlayer)
                else:
                    mergedlayer = img.dl()
                self.layerfile.delete()
                self.layerfile.put(pygame.image.tostring(mergedlayer._mSurface, "RGBA"))
                #TODO, make layerfile a compressed object
            #self._imgcache = ''
        
        results = self.results
        self.results = []

        super(Frame, self).save(*args, **kwargs)
        
        #TODO, this is sloppy -- we should handle this with cascading saves
        #or some other mechanism
        for r in results:
            if not r.frame:  
                r.frame = self.id
            r.save(*args, **kwargs)  #TODO, check to make sure measurement/inspection are saved
        
    def serialize(self):
        s = StringIO()
        try:
            self.image.save(s, "webp", quality = 80)
            return dict(
                content_type='image/webp',
                data=s.getvalue())
        except KeyError:
            self.image.save(s, "jpeg", quality = 80)
            return dict(
                content_type='image/jpeg',
                data=s.getvalue())
