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
import numpy as np

log = logging.getLogger(__name__)


#################################
# name: Name of the OLAP object
# maxLen: Maximum numer of results to return, aggregate if number exceeds limit
# groupTime: used for aggreagating result.  Possible values are: minute, hour, day
# valueMap: used to map output values to other values.  dict where the key is substituted with the value.  e.g., {'red': 3} will look for red and replace with 3.
#       must include 'field': the name of the field to map, e.g., {'field': 'string'}
#       must include 'default': the value when no other entry in the map works
#       rest of fieds are key/val pairs for the substitution
# since: limit to results where capturetime greater than time specified.  Time in epoch seconds
# before: limit to results where capturetime less than time specified.  Time in epoch seconds
# olapFilter: filter parameters for OLAP, same as with Filters
# statsInfo: Select how to group and aggregate data.  List of dicts.  In each dict, the key is the name of the function and the val is the name of the field on which to apply the function
#       Allowed functions based on mongo aggregation framework, such as first, last, max, min, avg, sum
# notNull: specify a default integer value to fill in any missing fields
#################################

  
class OLAPSchema(fes.Schema):
    name = fev.UnicodeString()            
    maxLen = fev.Int()
    groupTime = fev.UnicodeString()
    valueMap = V.JSON(if_empty=dict, if_missing=None)
    since = fev.Int()
    before = fev.Int()
    olapFilter = V.JSON(if_empty=dict, if_missing=None)
    statsInfo = V.JSON(if_empty=dict, if_missing=None)
    notNull = fev.Int()
    
class OLAP(SimpleDoc, mongoengine.Document):

    name = mongoengine.StringField()
    maxLen = mongoengine.IntField()
    groupTime = mongoengine.StringField()
    valueMap = mongoengine.ListField()
    since = mongoengine.IntField()
    before = mongoengine.IntField()
    olapFilter = mongoengine.ListField()
    statsInfo = mongoengine.ListField()
    notNull = mongoengine.IntField()
    transient = mongoengine.BooleanField()
    confirmed = mongoengine.BooleanField()
    
    meta = {
        'indexes': ['name']
    }


    def __repr__(self):
        return "<OLAP %s>" % self.name

    def execute(self, filterParams = [], realtime=False):
        
        filterParams = self.mergeParams(filterParams)
        
        # Get the raw data
        results = self.doQuery(filterParams, realtime=realtime)
        
        # Run any descriptive statistics or aggregation
        results = self.doStats(results)
        
        # Handle auto-aggregation
        if len(results) > self.maxLen:
            results = self.autoAggregate(results)
        
        # If necessary, remap the values in post processing
        results = self.doPostProc(results)
        
        # Check for empty results and handle if necessary
        if not len(results) and type(self.notNull) == int:
            results = self.defaultOLAP()
        
        # Convert Pandas DataFrame into dict
        return [v for v in results.transpose().to_dict().values()]
    
    def mergeParams(self, passedParams):
        # Take the passed parameters and override the built-in parameters 
        merged = []
        for f in self.olapFilter:
            filtFound = 0
            for p in passedParams:
                if p['type'] == f['type'] and p['name'] == f['name']:
                    merged.append(p)
                    filtFound = 1
            if not filtFound: merged.append(f)
        
        return merged
        
    def doPostProc(self, results):
        # Remap fields if necessary
        for vmap in self.valueMap:
            field = vmap['field']
            default = vmap['default']
            newvals = vmap['valueMap']
            
            results[field] = results[field].apply(lambda x: newvals.get(x, default))
        
        return results

    def doStats(self, results):
        for stat in self.statsInfo:
            field = stat['field']
            fn = stat['fn']
            param = stat['param']
            
            if fn[:7] == 'rolling':
                # Rolling stats don't change the size of the data, so can just add the field
                pdFunc = pd.__getattribute__(fn)
                results[field + '.' + fn] = pdFunc(results[field], param)
            else:
                # First, have to handle the treatment of different types of variables
                # If it is a numeric variable, (np.float64), do the actual stats function specified
                # Else, take the first element from the series
                keyFuncs = {}
                for key in results.keys():
                    if type(results[key][0]) == np.float64:
                        keyFuncs[key] = np.__getattribute__(fn)
                    else:
                        keyFuncs[key] = self.firstNotNan #lambda x: [type(y) for y in x]
                
                # Group splits the data frame up into bins
                grouped = results.groupby(lambda x: results[field][x].__getattribute__(param))
                # And run the functions
                results = grouped.agg(keyFuncs)
                
        return results

    def firstNotNan(self, x):
        for y in x:
            if type(y) != float: return y
        return 0

    def doQuery(self, filterParams, realtime = False):
        # Run the query, returning the results as a Pandas dataframe.
        # All the heavy lifting now done by Filters
        f = Filter()
        
        count, frames = f.getFrames(filterParams, realtime=realtime)
        flat = f.flattenFrame(frames)
        
        return pd.DataFrame(flat)

    def autoAggregate(self, resultSet, autoUpdate = True):
        oldest = resultSet[-1]
        newest = resultSet[0]
        
        elapsedTime = (newest['capturetime'] - oldest['capturetime']).total_seconds()
        timeRange = elapsedTime / self.maxLen
            
        # Set the grouping interval
        if timeRange < 60: groupTime = 'minute'
        elif timeRange < 3600: groupTime = 'hour'
        else: groupTime = 'day'

        # Decide how to group
        # If already grouped (has stats info), don't change it
        self.statsInfo.append({'field':'capturetime', 'fn': 'mean', 'param': groupTime})    
            
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


    
