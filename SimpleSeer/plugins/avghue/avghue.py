import numpy as np

import SimpleCV
from SimpleSeer import models as M
from SimpleSeer import util

from SimpleSeer.plugins import base

"""
Overly simplified avg hue plugin
insp = Inspection( name= "Motion", method="motion", camera = "Default Camera")
insp.save()

meas = Measurement( name="movement", label="Movement", method = "movement", parameters = dict(), units = "", featurecriteria = dict( index = 0 ), inspection = insp.id )
meas.save()


"""

class HueAvgFeature(SimpleCV.Feature):
  hue = 0.0
 
  hueAsBGR = (0,0,0)
  
  hueAsRGB = (0,0,0)

  def __init__(self, image, hue, BGR, ROI):
    #TODO, if parameters are given, crop on both images
    self.image = image
    self.hue = hue
    self.points = ROI
    self.asBGR = BGR
    self._updateExtents() # figure out all the crap from points/roi

class AvgHue(base.InspectionPlugin):

  def __call__(self, image, ROI=None )
    """
    ROI - region of interest as either 
    
    (top_left_x,top_left_y,width,height) => A classic rectangle
    (center_x, center_y, radius) => A circle
    [(x0,y0),(x1,y1),(x2,y2),...,(xN,yN)] => An open convex or concave polygon (no intersecting edges please).


    """
    SS = util.get_seer()
    if( ROI is not None):
      
      if( isinstance(ROI,tuple) and len(other)==3 ): # A circle
        src = image 
        mask = Image(image.width,image.height)
        target = Image(image.width,image.height)
        mask.drawCircle( (ROI[0],ROI[1]),ROI[2],color=Color.WHITE,thickness=-1)
        mask = mask.applyLayers()
        src = src.blit(target,mask=mask)
        r = ROI[2]
        # update the ROI to something that plays nicely
        ROI = [(ROI[0]-r,ROI[1]-r),
               (ROI[0]+r,ROI[1]-r),
               (ROI[0]+r,ROI[1]+r),
               (ROI[0]-r,ROI[1]+r)]
      elif( isinstance(ROI,tuple) and len(other)==4 ): # A Square
        src = image.crop( ROI[0],ROI[1],ROI[2],ROI[3])
        #update the roi to a way the features should like
        ROI = [(ROI[0],ROI[1]),
               (ROI[0]+ROI[2],ROI[1]),
               (ROI[0]+ROI[2],ROI[1]+ROI[3]),
               (ROI[0],ROI[1]+ROI[3])]

      elif( isinstance(ROI,list) and len(other) > 2 ): # A poly
        src = image 
        mask = Image(image.width,image.height)
        target = Image(image.width,image.height)
        mask.dl().polygon(ROI,color=Color.WHITE,filled=True)
        mask = mask.applyLayers()
        src = src.blit(target,mask=mask)
      else:
        return None
    else:
      ROI = [(0,0),
             (image.width,0),
             (image.widht,image.height),
             (0,image,height)]

    myHue = src.hueHistogram()
    myHue = np.argmax(h)

    #This is a hack, we need to add a color conversion macro 
    temp = Image((1,1),colorSpace=ColorSpace.HSV)
    temp[0,0] = (myHue,255,255)
    temp = temp.toBGR()
    BGR = temp[0,0]
    
    ff = M.FrameFeature()
    #we'll keep the feature agnostic of the weird ROIs for right now
    ff.setFeature(HueFeature(image, myHue,BGR,ROI))
    return [ff]
