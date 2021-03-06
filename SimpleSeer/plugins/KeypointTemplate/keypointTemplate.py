import numpy as np

from SimpleCV import *
from SimpleSeer import models as M
from SimpleSeer import util

from SimpleSeer.plugins import base
"""
Overly simplified template matching plugin

insp = Inspection( name= "KeypointTemplate", 
                   method="KeypointTemplate", 
                   camera = "Default Camera")
insp.save()


meas = Measurement( name="center", 
                    label="position", #Human readable name 
                    method = "center", #the method to call on a regionFeature
                    parameters = dict(), #not needed - store parameters here
                    units = "pixels", # 
                    featurecriteria = dict( index = 0 ), #not used
                    inspection = insp.id #point back to the original inspection)

meas.save()

#Inspection(name="derp7",method="keypointTemplate",parameters={"template":"/home/kscottz/SimpleSeer/SimpleSeer/plugins/KeypointTemplate/template.png"}).save()
"""

class KeypointTemplate(base.InspectionPlugin):
  """
  KeypointTemplate
  
  extraction parameters:
  method = method string - ignore this if you don't know what it is 
  threshold = the top x standard deviations above the mean match, in general 3/4/5 work well
  template = A list of templates from gridfs

  """
  def __call__(self, image):
    params = util.utf8convert(self.inspection.parameters)

    retVal = []
    #we assume all of this is validated and correct 
    templates = [] # get templates from GridFS  
    quality = 500.00
    minDist = 0.2
    minMatch = 0.4

    #this is a temporary hack
    if( not params.has_key('template') ):
      print "Bailing due to lack of template."
      return [] # required param
    else:
      templ=Image(params['template'])
      templates=[templ]



    if( params.has_key('quality') ):
      quality = params['quality'] 

    if( params.has_key('minDist') ):
      minDist = params['minDist']

    if( params.has_key('minMatch') ):
      minMatch = params['minMatch'] 
      
    # we want to optionally supply a count value, if we don't get count
    # number of hits we iterate through the templates, try to match up the 
    # overlapping ones, and then get a final count. 
      
    for t in templates:
      fs = image.findKeypointMatch(t,quality,minDist,minMatch)
      if fs is not None:
        break 

    if fs is not None:
      fs.draw()
      for f in fs: # do the conversion from SCV featureset to SimpleSeer featureset
        f._homography = None
        f._template = None
        f._avgColor = None
        #print f._minRect
        #print type(f._minRect)
        f.contour = [f._minRect[0],f._minRect[1],f._minRect[2],f._minRect[3]]
        ff = M.FrameFeature()
        ff.setFeature(f)
        retVal.append(ff)

    if( params.has_key("saveFile") ):
      image.save(params["saveFile"])

    return retVal 

