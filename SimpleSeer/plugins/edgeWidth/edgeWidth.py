import numpy as np

from SimpleCV import *
from SimpleSeer import models as M
from SimpleSeer import util

from SimpleSeer.plugins import base

"""
Overly simplified motion detection plugin
insp = Inspection( name= "EdgeWidth", method="edgeWidth", camera = "Default Camera")
insp.save()

meas = Measurement( name="movement", label="Movement", method = "movement", parameters = dict(), units = "", featurecriteria = dict( index = 0 ), inspection = insp.id )
meas.save()


"""

class EdgeWidthFeature(SimpleCV.Line):
  distance = 0 #the distance between the two intersection points
  intersectionPts = [] # the two points of intersection
  sourceLinePts = [] # the two points that define the line where we look for intersections

  def __init__(self, image, srcPts,intersections ):
    #TODO, if parameters are given, crop on both images
    self.image = image
    self.end_points = intersections
    self.sourceLinePts = srcPts 
    dx = intersections[0][0] - intersections[1][0]
    dy = intersections[0][1] - intersections[1][1]
    self.distance = np.sqrt( (dx*dx)+(dy*dy) )
    line = intersections 
    at_x = (line[0][0] + line[1][0]) / 2
    at_y = (line[0][1] + line[1][1]) / 2
    xmin = np.min([line[0][0],line[1][0]])
    xmax = np.max([line[0][0],line[1][0]])
    ymax = np.min([line[0][1],line[1][1]])
    ymin = np.max([line[0][1],line[1][1]])
    points = [(xmin,ymin),(xmin,ymax),(xmax,ymax),(xmax,ymin)]
    super(EdgeWidthFeature, self).__init__(image,points)
    
class EdgeWidth(base.InspectionPlugin):
  
  @classmethod
  def coffeescript(cls):
    yield 'models/feature', '''
class EdgeWidthCSFeature
  constructor: (feature) ->
    @feature = feature
   
  
  icon: () => "/img/edgeWidth.png" 
    
  represent: () =>
    "Edge Width mesasured from (" +
    @feature.get("featuredata").edge_points[0][0] + " , " +
    @feature.get("featuredata").edge_points[0][1] + ") to ( " + 
    @feature.get("featuredata").edge_points[1][0] + " , " +
    @feature.get("featuredata").edge_points[1][1] + ") length is"  +
    @feature.get("featuredata").line_length + "."

  tableOk: => true
    
  tableHeader: () =>
    ["X Start", "Y Start", "X End", "Y End" ,"Length"]
    
  tableData: () =>
    [@feature.get("featuredata").end_points[0][0],
     @feature.get("featuredata").end_points[0][1],
     @feature.get("featuredata").end_points[1][0],
     @feature.get("featuredata").end_points[1][1],
     @feature.get("featuredata").line_length]
    
  render: (pjs) =>
    pjs.stroke 0, 180, 180
    pjs.strokeWeight 3
    pjs.noFill()
    pjs.line( @feature.get('featuredata').end_points[0][0],
              @feature.get('featuredata').end_points[0][1],
              @feature.get('featuredata').end_points[1][0],
              @feature.get('featuredata').end_points[1][1] )

plugin this, EdgeWidthFeature:EdgeWidthCSFeature
'''
  def __call__(self, image):
    pt0 = None
    pt1 = None 
    canny1 = 0
    canny2 = 100 
    width = 3
    
    params = util.utf8convert(self.inspection.parameters)    

    if( params.has_key("pt0")):
      pt0 = params["pt0"]
    else: # required
      return []
    
    if( params.has_key("pt1")):
      pt1 = params["pt1"]
    else: # required
      return []
      
    if( params.has_key("canny")):
      canny = params["canny"]
      canny1 = canny[0]
      canny2 = canny[1]
      
    if( params.has_key("width")):
      width = params["width"]
      
    result = image.edgeIntersections(pt0,pt1,width,canny1,canny2)
        
    retVal = []
    if(result[0] is not None and
       result[1] is not None ):
      ff = M.FrameFeature()
      ewf = EdgeWidthFeature(image,[pt0,pt1],result)
      ff.setFeature(ewf)
      retVal.append(ff)
    
    if( params.has_key("saveFile") ):
      if(result[0] is not None and 
         result[1] is not None ):
        image.drawCircle( result[0],10,color=Color.RED)
        image.drawCircle( result[1],10,color=Color.RED)
        image.drawLine( pt0, pt1,thickness=width,color=Color.GREEN)
        image.save(params["saveFile"])  

    return retVal
