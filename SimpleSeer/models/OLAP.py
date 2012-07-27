import logging

import mongoengine
from .base import SimpleDoc

from formencode import validators as fev
from formencode import schema as fes
from SimpleSeer import validators as V
import formencode as fe

from datetime import datetime, timedelta
from SimpleSeer.Filter import Filter

from SimpleSeer.models.Measurement import Measurement
from SimpleSeer.models.Inspection import Inspection

import pandas as pd

log = logging.getLogger(__name__)


#################################
# name: Name of the OLAP object
# maxLen: Maximum numer of results to return, aggregate if number exceeds limit
# queryType: type of query: measurement or inspection.  e.g., 'measurement'
# queryId: the id of the object as referenced in previous step.  e.g., ObjectId('4fdbac481d41c834fb000001')
# fields: a list of fields to be returned.  e.g., ['capturetime', 'numeric', 'measurement']
# groupTime: time groupby interval: minute, hour, day
# valueMap: used to map output values to other values.  dict where the key is substituted with the value.  e.g., {'red': 3} will look for red and replace with 3.
#               must include 'field': the name of the field to map, e.g., {'field': 'string'}
#               must include 'default': the value when no other entry in the map works
#               rest of fieds are key/val pairs for the substitution
# since: limit to results where capturetime greater than time specified.  Time in epoch seconds
# before: limit to results where capturetime less than time specified.  Time in epoch seconds
# customFilter: dict of additional filter values.  e.g., {'string': 'yellow'}
# groupTime: used for aggreagating result.  Possible values are: minute, hour, day
# statsInfo: Select how to group and aggregate data.  List of dicts.  In each dict, the key is the name of the function and the val is the name of the field on which to apply the function
#       Allowed functions based on mongo aggregation framework, such as first, last, max, min, avg, sum
# postProc: Stats function that require global data set (don't work well with mongo)
#       Allowed functions: movingCount
#################################

  
class OLAPSchema(fes.Schema):
    name = fev.UnicodeString()            
    maxLen = fev.Int()
    queryType = fev.UnicodeString()
    queryId = fev.UnicodeString()
    #queryIds = V.JSON(if_empty=[], if_missing=None)
    #fields = V.JSON(if_empty=dict, if_missing=None)
    #valueMap = V.JSON(if_empty=dict, if_missing=None)
    groupTime = fev.UnicodeString()
    since = fev.Int()
    before = fev.Int()
    #customFilter = V.JSON(if_empty=dict, if_missing=None)   
    #statsInfo = V.JSON(if_empty=dict, if_missing=None)
    #postProc = V.JSON(if_empty=dict, if_missing=None)
    notNull = fev.Int()
    #linkedOLAP = V.JSON()

class OLAP(SimpleDoc, mongoengine.Document):

    name = mongoengine.StringField()
    maxLen = mongoengine.IntField()
    queryType = mongoengine.StringField()
    queryId = mongoengine.ObjectIdField()
    queryIds = mongoengine.ListField()
    fields = mongoengine.ListField()
    groupTime = mongoengine.StringField()
    valueMap = mongoengine.ListField()
    since = mongoengine.IntField()
    before = mongoengine.IntField()
    customFilter = mongoengine.DictField()
    statsInfo = mongoengine.ListField()
    postProc = mongoengine.DictField()
    notNull = mongoengine.IntField()
    linkedOLAP = mongoengine.ListField()
    
    meta = {
        'indexes': ['name']
    }


    def __repr__(self):
        return "<OLAP %s>" % self.name

    def execute(self, filterParams = {}):
        
        # Get the raw data
        results = self.doQuery(filterParams)
        
        #if len(results) > self.maxLen:
        #    results = self.autoAggregate(results)
        
        
        
        results = self.doPostProc(results)
        results = self.doStats(results)
        
        if not len(results) and type(self.notNull) == int:
            results = self.defaultOLAP()
        
        return [v for v in results.transpose().to_dict().values()]
        
        
    def doPostProc(self, results, realtime=False):
        
        for vmap in self.valueMap:
            field = vmap['field']
            default = vmap['default']
            newvals = vmap['valueMap']
            
            results[field] = results[field].apply(lambda x: newvals.get(x, default))
            results[field] = results[field].apply(lambda x: newvals.get(x, default))
        
        return results

    def doStats(self, results, realtime=False):
        return results

        # Re implement this stuff
        if 'movingCount' in self.postProc:
            if realtime:
                full_res = self.doQuery()
                results['movingCount'] = len(full_res) + 1
                
            else:
                for counter, r in enumerate(results):
                    r['movingCount'] = counter + 1


    def doQuery(self, filterParams):
        f = Filter()
        
        queryParams = filterParams
        if self.queryType == 'measurement_id':
            m = Measurement.objects(id=self.queryId)[0]
            queryParams = {'type': 'measurement', 'name': m.name}
        elif self.queryType == 'inspection_id':
            log.warn('Can not currently run olaps on inspections')
            # TODO: Implement this
        
        count, frames = f.getFrames([queryParams], unit='result')
        flat = f.flattenFrame(frames)
        
        return pd.DataFrame(flat)

  
  
    def autoAggregate(self, resultSet, autoUpdate = True):
        oldest = resultSet[-1]
        newest = resultSet[0]
        
        elapsedTime = (newest['capturetime'] - oldest['capturetime']).total_seconds()
        timeRange = elapsedTime / self.maxLen
            
        # Set the grouping interval
        if timeRange < 60: self.groupTime = 'minute'
        elif timeRange < 3600: self.groupTime = 'hour'
        else: self.groupTime = 'day'

        # Decide how to group
        # If already grouped (has stats info), don't change it
        if len(self.statsInfo) == 0:
            # For string items, use the last element in the group
            # For numeric items, take the average
            for key, val in oldest.iteritems():
                
                if not key == '_id':
                    if (type(val) == int) or (type(val) == float):
                        self.statsInfo.append({'avg': key})
                    else:
                        self.statsInfo.append({'first': key})
        
        if autoUpdate:
            self.save()
            return self.doQuery()
        
        else:
            return []

    def defaultOLAP(self):
        from bson import ObjectId
        # Returns data set of all default values, formatted for this olap
        
        fakeResult = {}
        
        for f in self.fields:
            if f == self.queryType:
                fakeResult[f] = self.queryId
            elif f[-2:] == 'id':
                fakeResult[f] = ObjectId()
            elif f == 'capturetime':
                fakeResult[f] = datetime.utcnow()
            else:
                fakeResult[f] = 0
                
        fakeResult['_id'] = ObjectId()
                
        fake2 = fakeResult.copy()
        fake2['capturetime'] -= timedelta(0,2)       
                
        return [fakeResult, fake2]


    
