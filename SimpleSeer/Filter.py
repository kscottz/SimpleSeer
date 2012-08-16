import logging

from .models.Frame import Frame
from .models.Inspection import Inspection
from .models.Measurement import Measurement
from datetime import datetime
from calendar import timegm

log = logging.getLogger(__name__)

class Filter():
    
    def getFrames(self, allFilters, skip=0, limit=float("inf"), sortinfo = {}):
        
        pipeline = []
        frames = []
        measurements = []
        features = []
        
        # Need to initially construct/modify a few fields for future filters
        pipeline += self.initialFields()
        
        # Filter the data based on the filter parameters
        # Frame features are easy to filter, but measurements and features are embedded in the frame
        # so they need their own syntax to filter
        for f in allFilters:
            if f['type'] == 'measurement':
                measurements.append(f)
            elif f['type'] == 'frame':
                frames.append(f)
            elif f['type'] == 'framefeature':
                features.append(f)
                
        if frames:
            pipeline += self.filterFrames(frames)
        
            
        pipeline += self.filterMeasurements(measurements)            
        pipeline += self.filterFeatures(features)
        
        # Sort the results
        pipeline += self.sort(sortinfo)
        
        #for p in pipeline:
        #    print 'LINE: %s' % str(p)
        
        # This is all done through mongo aggregation framework
        db = Frame._get_db()
        cmd = db.command('aggregate', 'frame', pipeline = pipeline)
        results = cmd['result']
        
        # Perform the skip/limit 
        # Note doing this in python instead of mongo since need original query to give total count of relevant results
        if skip < len(results):
            if (skip + limit) > len(results):
                results = results[skip:]
            else:
                results = results[skip:skip+limit]
        else:
            return 0, []
        
        for r in results:
            r['capturetime'] = timegm(r['capturetime'].timetuple()) * 1000
            
        return len(cmd['result']), results    
        
    def initialFields(self):
        # This is a pre-filter of the relevant fields
        # It constructs a set of fields helpful when grouping by time
        # IT also constructs a set of custom renamed fields for use by other filters
        
        fields = {}
        
        # First select the fields from the frame to include
        for p in Frame.filterFieldNames():
            fields[p] = 1
        
        # And we always need the features and results
        fields['features'] = 1
        fields['results'] = 1
        
        return [{'$project': fields}]
    
    
    def recurseMap(self, fieldName, defaultVal, remainTerms):
        # This function is used by the initialFields function
        # It constructs a nested conditional statement for remapping field values
        
        if len(remainTerms) > 0:
            key, val = remainTerms.popitem()            
            return {'$cond': [{'$eq': [fieldName, key]}, val, self.recurseMap(fieldName, defaultVal, remainTerms)]}
        else:
            return defaultVal

    def filterFrames(self, frameQuery):
        # Construct the filter based on fields in the Frame object
        # Note that all timestamps are passed in as epoch milliseconds, but
        # fromtimestamp() assumes they are in seconds.  Hence / 1000
 
        filters = {}
        for f in frameQuery:    
            if 'eq' in f:
                if f['name'] == 'capturetime':
                    f['eq'] = datetime.fromtimestamp(f['eq'] / 1000)
                comp = f['eq']
            else:
                comp = {}
                if 'gt' in f and f['gt']:
                    if f['name'] == 'capturetime':
                        f['gt'] = datetime.fromtimestamp(f['gt'] / 1000)
                    comp['$gt'] = f['gt']
                if 'lt' in f and f['lt']:
                    if f['name'] == 'capturetime':
                        f['lt'] = datetime.fromtimestamp(f['lt'] / 1000)
                    comp['$lt'] = f['lt']
             
            filters[f['name']] = comp
         
        return [{'$match': filters}]
    
    def filterMeasurements(self, measQuery):
        # Do the basic pipeline construction for filtering on Measurements
        # (which appear in Frames under $results)
        # Always unwind to filter out unneded fields from results
        
        parts = []
        
        proj, group = self.rewindFields('results')
        
        # If measurements query, check those fields
        if measQuery:
            proj['measok'] = self.condMeas(measQuery)
            group['allmeasok'] = {'$sum': '$measok'}
        
        parts.append({'$unwind': '$results'})
        parts.append({'$project': proj})
            
        ## If the unit of analysis is not 'results', re-group the result objects and filter at the group level
        #if unit != 'result':
        parts.append({'$group': group})
        if measQuery:
            parts.append({'$match': {'allmeasok': len(measQuery)}})
    
        #elif measQuery:
        #    parts.append({'$match': {'measok': 1}})
        
        return parts
    
    
    def filterFeatures(self, featQuery):
        # Do the basic pipeline construction for filtering on features
        
        parts = []
        proj, group = self.rewindFields('features')
        
        if featQuery:
            proj['featok'] = self.condFeat(featQuery)
            group['allfeatok'] = {'$sum': '$featok'}
            
        parts.append({'$unwind': '$features'})
        parts.append({'$project': proj})
        
        #if unit != 'feature':
        parts.append({'$group': group})
        if featQuery:
            parts.append({'$match': {'allfeatok': len(featQuery)}})
        #elif featQuery:
        #    parts.append({'$match': {'featok': 1}})
        
        return parts
    
    def sort(self, sortinfo):
        # Sort based on specified parameters
        # Sorting may be done on fields inside the results or features
        
        parts = []
        
        if sortinfo:
            sortinfo['order'] = int(sortinfo['order'])
            if sortinfo['type'] == 'measurement':
                parts.append({'$sort': {'results.numeric': sortinfo['order'], 'results.string': sortinfo['order']}})
            elif sortinfo['type'] == 'framefeature':
                feat, c, field = sortinfo['name'].partition('.')
                parts.append({'$sort': {'features.' + field: sortinfo['order']}})
            else:
                parts.append({'$sort': {sortinfo['name']: sortinfo['order']}})
        else:
            parts.append({'$sort': {'capturetime': 1}})
        
        return parts
    
    def rewindFields(self, field):
        # Handle the grouping when undoing the unwind operations
        # Also filters out unnecessary fields from embedded docs to keep results smaller
        
        proj = {}
        group = {}
        
        # Only keep those keys requested
        featKeys, resKeys = self.keyNamesHash()
        
        if field == 'results':
            useKeys = resKeys
        elif field == 'features':
            useKeys = featKeys
            
        for key in useKeys:
            for f in useKeys[key]:
                proj[field + '.' + f] = 1
        
        
        for key in Frame.filterFieldNames():
            # Have to rename the id field since $group statements assume existence of _id as the group_by parameter
            if key == 'id':
                key = '_id'
            proj[key] = 1
            
            #if (key == 'results') or (key == 'features'):
            if key == field:
                group[key] = {'$addToSet': '$' + key}
            else:
                group[key] = {'$first': '$' + key}
            
        group['_id'] = '$_id'
        # But a lot of stuff also wants an id instead of _id
        group['id'] = {'$first': '$_id'}

        return proj, group
    
    def condMeas(self, measurements):
        
        allfilts = []
        for m in measurements:    
            meas, c, field = m['name'].partition('.')
            
            comp = []
            if 'eq' in m:
                comp.append({'$eq': ['$results.' + field, str(m['eq'])]})
            if 'gt' in m:
                comp.append({'$gte': ['$results.' + field, m['gt']]})
            if 'lt' in m:
                comp.append({'$lte': ['$results.' + field, m['lt']]})
            if 'exists' in m:
                comp.append('$results.' + field)
                
            comp.append({'$eq': ['$results.measurement_name', meas]})
            combined = {'$and': comp}
            allfilts.append(combined)
                
        return {'$cond': [{'$or': allfilts}, 1, 0]}
        
        
    def condFeat(self, features):
        
        allfilts = []
        for f in features:
            feat, c, field = f['name'].partition('.')
            
            comp = []
            if 'eq' in f:
                comp.append({'$eq': ['$features.' + field, str(f['eq'])]})
            if 'gt' in f:
                comp.append({'$gte': ['$features.' + field, f['gt']]})
            if 'lt' in f:
                comp.append({'$lte': ['$features.' + field, f['lt']]})
            if 'exists' in f:
                comp.append('$features.' + field)
                    
            comp.append({'$eq': ['$features.featuretype', str(feat)]})
            combined = {'$and': comp}
            allfilts.append(combined)
            
        return {'$cond': [{'$or': allfilts}, 1, 0]}
        
        
    def checkFilter(self, filterType, filterName, filterFormat):
        # Given information about a filter, checks if that field
        # exists in the database.  If so, provides the fitler
        # parameters, such as lower/upper bounds, or lists of options
        
        from datetime import datetime
        
        if not filterFormat in ['numeric', 'string', 'autofill', 'datetime']:
            return {"error":"unknown format"}
        if not filterType in ['measurement', 'frame', 'framefeature']:
            return {"error":"unknown type"}
            
        db = Frame._get_db()
        
        pipeline = []
        collection = ''
        field = ''
        
        if filterType == 'frame':
            collection = 'frame'    
            field = filterName
        elif filterType == 'measurement':
            collection = 'result'
            field = filterFormat
            if (field == 'autofill'):
                field = 'string'
            
            pipeline.append({'$match': {'measurement_name': filterName}})
            
        elif filterType == 'framefeature':
            feat, c, field = filterName.partition('.')
            field = 'features.' + field
            collection = 'frame'
        
            pipeline.append({'$unwind': '$features'})
            pipeline.append({'$match': {'features.featuretype': feat}})
            
        if (filterFormat == 'numeric') or (filterFormat == 'datetime'):
            pipeline.append({'$group': {'_id': 1, 'min': {'$min': '$' + field}, 'max': {'$max': '$' + field}}})
        
        if (filterFormat == 'autofill'):
            pipeline.append({'$group': {'_id': 1, 'enum': {'$addToSet': '$' + field}}})    
            
        if (filterFormat == 'string'):
            pipeline.append({'$group': {'_id': 1, 'found': {'$sum': 1}}})
        
        cmd = db.command('aggregate', collection, pipeline = pipeline)
        
        ret = {}
        if len(cmd['result']) > 0:
            for key in cmd['result'][0]:
                if type(cmd['result'][0][key]) == list:
                    cmd['result'][0][key].sort()
                
                if type(cmd['result'][0][key]) == datetime:
                    cmd['result'][0][key] = int(float(cmd['result'][0][key].strftime('%s.%f')) * 1000)
                if not key == '_id':
                    ret[key] = cmd['result'][0][key]
        else:
            return {"error":"no matches found"}
                
        return ret
        
    
    def toCSV(self, rawdata):
        import StringIO
        import csv
        
        # csv libs assume saving to a file handle
        f = StringIO.StringIO()
        
        # Convert the dict to csv
        csvWriter = csv.writer(f)
        csvWriter.writerows(rawdata)
        
        # Grab the string version of the output
        output = f.getvalue()
        f.close()
        
        return output
        
    def toExcel(self, rawdata):
        import StringIO
        from xlwt import Workbook, XFStyle
        
        # Need a file handle to save to
        f = StringIO.StringIO()
        
        # Construct a workbook with one sheet
        wb = Workbook()
        s = wb.add_sheet('export')
        
        # Write the data
        for i, data in enumerate(rawdata):
            for j, val in enumerate(data):
                s.write(i, j, val)
                
        # Save the the string IO and grab the string data
        wb.save(f)
        output = f.getvalue()
        f.close()
        
        return output
        
    def keyNamesHash(self):
        # find all possible feature and result names
        
        featureKeys = {}
        resultKeys = {}
        
        for i in Inspection.objects:
            # Features can override their method name
            # To get actual plugin name, need to go through the inspection
            # Then use plugin to find the name of its printable fields
            plugin = i.get_plugin(i.method)
            if 'printFields' in dir(plugin):
                featureKeys[i.name] = plugin.printFields()
                # Always make sure the featuretype and inspection fields listed for other queries
                featureKeys[i.name].append('featuretype')
                featureKeys[i.name].append('inspection')
            else:
                featureKeys[i.name] = ['featuretype', 'inspection', 'featuredata']
                
        # Becuase of manual measurements, need to look at frame results to figure out if numeric or string fields in place
        for m in Measurement.objects:
            # Have some manual measurements, which lack an actual plugin
            # Will have to ignore these for now, but log the issue                    
            try:
                plugin = m.get_plugin(m.method)
                if 'printFields' in dir(plugin):
                    resultKeys[m.name] = plugin.printFields()
                    resultKeys[m.name].append('measurement_name')
                else:
                    resultKeys[m.name] = ['measurement_name', 'measurement_id', 'inspection_id', 'string', 'numeric']
            except ValueError:
                # log.info('No plugin found for %s, using default fields' % m.method)
                resultKeys[m.name] = ['measurement_name', 'measurement_id', 'inspection_id', 'string', 'numeric']
        
        
        return featureKeys, resultKeys
        

    def keyNamesList(self):
        featureKeys, resultKeys = self.keyNamesHash()
        
        fieldNames = Frame.filterFieldNames()
                
        for key in featureKeys.keys():
            for val in featureKeys[key]:
                fieldNames.append(key + '.' + val)
            
        for key in resultKeys.keys():
            for val in featureKeys[key]:
                fieldNames.append(key + '.' + val)
            
        return fieldNames
        
    
    @classmethod
    def unEmbed(self, frame):
        feats = frame['features']
        newFeats = []
        for f in feats:
            newFeats.append(f['py/state'])
        frame['features'] = newFeats
        
        results = frame['results']
        newRes = []
        for r in results:
            newRes.append(r['py/state'])
        frame['results'] = newRes
        
        return frame
    
    def getField(self, field, keyParts):
        # This function recursively pulls apart the key parts to unpack the hashes and find the actual value
                
        if len(keyParts) == 1:
            return field.get(keyParts[0], None)
        else:
            return self.getField(field.get(keyParts.pop(0), {}), keyParts) 
    
    def inspectionIdToName(self, inspId):
        return Inspection.objects(id=inspId)[0].name
    
    def flattenFrame(self, frames):
        
        featureKeys, resultKeys = self.keyNamesHash()
        
        flatFrames = []
        for frame in frames:
            tmpFrame = {}
            
            # Grab the fields from the frame itself
            for key in Frame.filterFieldNames():
                if key == '_id' and 'id' in frame:
                    key = 'id'
                
                keyParts = key.split('.')
                tmpFrame[key] = self.getField(frame, keyParts)
                
            # Fields from the features
            for feature in frame['features']:
                # If this feature has items that need to be saved
                inspection_name = self.inspectionIdToName(feature['inspection']) 
                if  inspection_name in featureKeys.keys():
                    # Pull up the relevant keys, named featuretype.field
                    for field in featureKeys[inspection_name]:
                        keyParts = field.split('.')
                        tmpFrame[feature['featuretype'] + '.' + field] = self.getField(feature, keyParts)
             
            # Fields from the results
            for result in frame['results']:
                # If this result has items that need to be saved
                if result['measurement_name'] in resultKeys.keys():
                    for field in resultKeys[result['measurement_name']]:
                        keyParts = field.split('.')
                        tmpFrame[result['measurement_name'] + '.' + field] = self.getField(result, keyParts)
                            
            flatFrames.append(tmpFrame)
            
        return flatFrames
