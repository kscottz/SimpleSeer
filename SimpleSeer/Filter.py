import logging

from .models.Frame import Frame
from .models.Inspection import Inspection
from .models.Measurement import Measurement
from datetime import datetime

log = logging.getLogger(__name__)

class Filter():
    
    fieldNames = ['_id', 'camera', 'capturetime', 'results', 'features', 'metadata']
    
    def getFrames(self, allFilters, unit='frame', skip=0, limit=float("inf"), sortinfo = {}, statsInfo = {}, groupTime = '', valueMap = {}):
        
        pipeline = []
        frames = []
        measurements = []
        features = []
        
        # Need to initially construct/modify a few fields for future filters
        pipeline += self.initialFields(groupTime, valueMap)
        
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
        
            
        pipeline += self.filterMeasurements(measurements, unit)            
        pipeline += self.filterFeatures(features, unit)
        
        # Sort the results
        pipeline += self.sort(sortinfo)
        
        for p in pipeline:
            print 'LINE: %s' % str(p)
        
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
        
        return len(cmd['result']), results    
        
    def initialFields(self, groupTime, valueMap):
        # This is a pre-filter of the relevant fields
        # It constructs a set of fields helpful when grouping by time
        # IT also constructs a set of custom renamed fields for use by other filters
        
        fields = {}
        
        # First select the fields from the frame to include
        for p in self.fieldNames:
            fields[p] = 1
        
        # And we always need the features and results
        fields['features'] = 1
        fields['results'] = 1
        
        # Construct a custom time field if need to group by time        
        if groupTime:
            if groupTime == 'minute':
                fields['t'] = { '$isoDate': { 'year': { '$year': '$capturetime' }, 
                                    'month': { '$month': '$capturetime' }, 
                                    'dayOfMonth': { '$dayOfMonth': '$capturetime' }, 
                                    'hour': { '$hour': '$capturetime' },
                                    'minute': { '$minute': '$capturetime'}}}
            elif groupTime == 'hour':
                fields['t'] = { '$isoDate': { 'year': { '$year': '$capturetime' }, 
                                    'month': { '$month': '$capturetime' }, 
                                    'dayOfMonth': { '$dayOfMonth': '$capturetime' }, 
                                    'hour': { '$hour': '$capturetime' }}}
            elif groupTime == 'day':
                fields['t'] = { '$isoDate': { 'year': { '$year': '$capturetime' }, 
                                    'month': { '$month': '$capturetime' }, 
                                    'dayOfMonth': { '$dayOfMonth': '$capturetime' }}}
 
        return [{'$project': fields}]
    
    
    def recurseMap(self, fieldName, defaultVal, remainTerms):
        # This function is used by the initialFields function
        # It constructs a nested conditional statement for remapping field values
        
        if len(remainTerms) > 0:
            key, val = remainTerms.popitem()            
            return {'$cond': [{'$eq': [fieldName, key]}, val, self.recurseMap(fieldName, defaultVal, remainTerms)]}
        else:
            return defaultVal

    def basicStats(self, statsInfo):
        stats = {}
        
        stats['_id'] = '$t'
        
        for s in self.statsInfo:
            key, val = s.items()[0]
            # Needs special handling for count
            if type(val) == int:
                stats['count'] = {'$sum': 1}
            else:
                stats[str(val)] = {'$' + str(key): '$' + str(val)}

        
        # TODO: Only groups on capturetime
        parts = []
        
        parts.append({'$sort': 'capturetime'})
        parts.append({'$group': stats})
        
        # Stats groups on _id field, which is inconsistent with non-grouped format
        # Use project to rename the _id field back to capturetime
        statsProject = {}
        
        for key, val in stats.iteritems():
            if key == '_id':
                statsProject['capturetime'] = '$_id'
                statsProject['_id'] = 0
            else:
                statsProject[key] = 1
        
        parts.append({'$project': statsProject})
        
        return parts
    
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
    
    def filterMeasurements(self, measQuery, unit):
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
    
    
    def filterFeatures(self, featQuery, unit):
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
        
        
        for key in self.fieldNames:
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
        
    
    def toCSV(self, frames):
        import StringIO
        import csv
        
        # csv libs assume saving to a file handle
        f = StringIO.StringIO()
        
        frames = self.flattenFrame(frames)
        
        keys = self.keyNamesList()
        
        # Convert the dict to csv
        csvDict = csv.DictWriter(f, keys)
        csvDict.writeheader()
        csvDict.writerows(frames)
        
        # Grab the string version of the output
        output = f.getvalue()
        f.close()
        
        return output
        
    def toExcel(self, frames):
        import StringIO
        from xlwt import Workbook, XFStyle
        
        # Need a file handle to save to
        f = StringIO.StringIO()
        
        frames = self.flattenFrame(frames)
        
        keys = self.keyNamesList()
        
        # Construct a workbook with one sheet
        wb = Workbook()
        s = wb.add_sheet('frames')
        
        # Create the style for date/time
        dateStyle = XFStyle()
        dateStyle.num_format_str = 'MM/DD/YYYY HH:MM:SS'
        
        # Add the header/field labels
        r = s.row(0)
        for i, name in enumerate(keys):
            r.write(i, name)
        
        # Write the data
        for i, frame in enumerate(frames):
            for j, name in enumerate(keys):
                try:
                    if type(frame[name]) == datetime:
                        s.write(i+1, j, frame[name], dateStyle)
                    else:
                        s.write(i+1, j, str(frame.get(name, 'N/A')))
                except KeyError:
                    pass_
            
        # Save the the string IO and grab the string data
        wb.save(f)
        output = f.getvalue()
        f.close()
        
        return output
        
    def keyNamesHash(self):
        # find all possible feature and result names
        
        featureKeys = {}
        resultKeys = {}
        
        Inspection.register_plugins('seer.plugins.inspection')

        for i in Inspection.objects:
            # Features can override their method name
            # To get actual plugin name, need to go through the inspection
            # Then use plugin to find the name of its printable fields
            try:
                plugin = i.get_plugin(i.method)
                if 'printFields' in dir(plugin):
                    featureKeys[i.name] = plugin.printFields()
                    # Always make sure the featuretype field is listed
                    featureKeys[i.name].append('featuretype')
                    print 'FOUND for ' + str(plugin)
                else:
                    featureKeys[i.name] = ['featuretype', 'featuredata']
                    print 'NOT FOUND'
            except ValueError:
                print 'NO PLUGIN'
                log.info('No plugin found for %s, using default fields' % i.method)
                featureKeys[i.name] = ['featuretype', 'featuredata']
                
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
                log.info('No plugin found for %s, using default fields' % m.method)
                resultKeys[m.name] = ['measurement_name', 'measurement_id', 'inspection_id', 'string', 'numeric']
        
        
        return featureKeys, resultKeys
        

    def keyNamesList(self):
        featureKeys, resultKeys = self.keyNamesHash()
        
        fieldNames = self.fieldNames
        
        for key in featureKeys.keys():
            for val in featureKeys[key]:
                fieldNames.append(key + '.' + val)
            
        for key in resultKeys.keys():
            for val in featureKeys[key]:
                fieldNames.append(key + '.' + val)
            
        return fieldNames
        
        
    def flattenFrame(self, frames):
        
        featureKeys, resultKeys = self.keyNamesHash()
        
        flatFrames = []
        for frame in frames:
            tmpFrame = {}
            
            # Grab the fields from the frame itself
            for key in self.fieldNames:
                if key == '_id' and 'id' in frame:
                    key = 'id'
                tmpFrame[key] = frame[key]
            
            # Fields from the features
            for feature in frame['features']:
                # If this feature has items that need to be saved
                if feature['featuretype'] in featureKeys.keys():
                    # Pull up the relevant keys, named featuretype.field
                    for field in featureKeys[feature['featuretype']]:
                        tmpFrame[feature['featuretype'] + '.' + field] = feature[field]
             
            # Fields from the results
            for result in frame['results']:
                # If this result has items that need to be saved
                if result['measurement_name'] in resultKeys.keys():
                    for field in resultKeys[result['measurement_name']]:
                        tmpFrame[result['measurement_name'] + '.' + field] = result[field]
                            
            flatFrames.append(tmpFrame)
            
        return flatFrames
	
		
