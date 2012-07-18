from .models.Frame import Frame
from datetime import datetime

FIELD_NAMES = ['camera', 'capturetime', 'height', 'width']

class Filter():
	
	def getFrames(self, allFilters, skip=0, limit=0, dictOutput=False):
		
		pipeline = []
		frames = []
		measurements = []
		features = []
		
		for f in allFilters:
			if f['type'] == 'measurement':
				measurements.append(f)
			elif f['type'] == 'frame':
				frames.append(f)
			elif f['type'] == 'framefeature':
				features.append(f)
		
		if frames:
			for f in frames:
				
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
				
				pipeline.append({'$match': {f['name']: comp}})
		
		if measurements:
			proj = {'measok': self.condMeas(measurements)}
			group = {'allmeasok': {'$sum': '$measok'}}
			
			
			for key in Frame._fields:
				if key == 'id':
					key = '_id'
				proj[key] = 1
				group[key] = {'$first': '$' + key}
			
			group['_id'] = '$_id'
		
			
			pipeline.append({'$unwind': '$results'})
			pipeline.append({'$project': proj})
			pipeline.append({'$group': group})
			pipeline.append({'$match': {'allmeasok': len(measurements)}})
			
		if features:
			proj = {'featok': self.condFeat(features)}
			group = {'allfeatok': {'$sum': '$featok'}}
			
			
			for key in Frame._fields:
				if key == 'id':
					key = '_id'
				proj[key] = 1
				group[key] = {'$first': '$' + key}
			
			group['_id'] = '$_id'
		
			
			pipeline.append({'$unwind': '$features'})
			pipeline.append({'$project': proj})
			pipeline.append({'$group': group})
			pipeline.append({'$match': {'allfeatok': len(features)}})
			
			
			
		pipeline.append({'$sort': {'capturetime': 1}})
		
		db = Frame._get_db()
		cmd = db.command('aggregate', 'frame', pipeline = pipeline)
		
		results = cmd['result']
		
		if skip < len(results):
			if (skip + limit) > len(results):
				results = results[skip:]
			else:
				results = results[skip:skip+limit]
		else:
			return 0, None, datetime(1970, 1, 1)
		
		earliest = results[0]['capturetime']
			
		
		# Check if need to output as dict or as Frames
		if dictOutput:
			return len(cmd['result']), results, earliest
			
		else:
			# Lookup the Frame objects by the ID frm the dict
			ids = [r['_id'] for r in results]
			frames = Frame.objects.filter(id__in=ids)
			
			# Conver the mongoengine queryset into a list of frames
			
			frs = [f for f in frames]
			
			return len(cmd['result']), frs, earliest
		
	
	def condMeas(self, measurements):
		
		allfilts = []
		for m in measurements:	
			
			comp = []
			if 'eq' in m:
				comp.append({'$eq': ['$results.string', str(m['eq'])]})
			
			if 'gt' in m:
				comp.append({'$gte': ['$results.numeric', m['gt']]})
			
			if 'lt' in m:
				comp.append({'$lte': ['$results.numeric', m['lt']]})	
				
			comp.append({'$eq': ['$results.measurement_name', str(m['name'])]})
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
					
			comp.append({'$eq': ['$features.featuretype', str(feat)]})
			combined = {'$and': comp}
			allfilts.append(combined)
			
		return {'$cond': [{'$or': allfilts}, 1, 0]}
		
		
		
	def checkFilter(self, filterType, filterName, filterFormat):
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
		
		# Convert the dict to csv
		csvDict = csv.DictWriter(f, FIELD_NAMES, extrasaction='ignore')
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
		
		# Construct a workbook with one sheet
		wb = Workbook()
		s = wb.add_sheet('frames')
		
		# Create the style for date/time
		dateStyle = XFStyle()
		dateStyle.num_format_str = 'MM/DD/YYYY HH:MM:SS'
		
		# Add the header/field labels
		r = s.row(0)
		for i, name in enumerate(FIELD_NAMES):
			r.write(i, name)
		
		# Write the data
		for i, frame in enumerate(frames):
			print type(frame['capturetime'])
			for j, name in enumerate(FIELD_NAMES):
				if type(frame[name]) == datetime:
					s.write(i+1, j, frame[name], dateStyle)
				else:
					s.write(i+1, j, frame[name])
		
		# Save the the string IO and grab the string data
		wb.save(f)
		output = f.getvalue()
		f.close()
		
		return output
		
		
