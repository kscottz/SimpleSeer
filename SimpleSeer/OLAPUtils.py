from .models.OLAP import OLAP
from .models.Chart import Chart
from .models.Measurement import Measurement
from .models.Inspection import Inspection

from .Filter import Filter

from gevent import Greenlet, sleep
from datetime import datetime, timedelta
from time import mktime

from .util import utf8convert
from .realtime import ChannelManager

from random import randint
import pandas as pd

import logging
log = logging.getLogger(__name__)


class OLAPFactory:
    
    @classmethod
    def confirmTransient(self, chartName):
        chart = Chart.objects(name=chartName)[0]
        olap = OLAP.objects(name=chart.olap)[0]
        olap.confirmed = True
        olap.save()
    
    def createTransient(self, filters, originalChart):
        # A transient OLAP is one that should be delted when no subscriptions are listening
        # This is needed for OLAPs that publish realtime but are the result of filters
        
        from .models.Chart import Chart
        
        # First, create the core OLAP
        originalOLAP = OLAP.objects(name = originalChart.olap)[0]
        o = self.fromFilter(filters, originalOLAP)
        o.transient = True
        o.confirmed = True
        o.save()
        
        # Create the chart to point to it
        # For a chart only used for realtime, we realy only care about the data maps
        c = Chart()
        c.name = originalChart.name + '_' + str(randint(1, 1000000))
        c.metaMap = originalChart.metaMap
        c.dataMap = originalChart.dataMap
        c.olap = o.name
        c.save()
    
    def fromFilter(self, filters, oldOLAP = None):
        
        newOLAP = OLAP()
        if oldOLAP:
            newOLAP.olapFilter = oldOLAP.mergeParams(filters)
        else:
            newOLAP.olapFilter = filters
            
        return self.fillOLAP(newOLAP)
    
    def fromFields(self, fields):
        # Create an OLAP object from a list of fields desired
        # Each field should be specified in the same was as Filter fields
        #   type: one of (frame, framefeature, measurement)
        #   name: if a frame, specify the field name
        #         otherwise use dotted notation for the frame/measurement name.field name
        
        for f in fields:
            f['exists'] = 1
        
        # Put together the OLAP
        o = OLAP()
        o.olapFilter = fields
        
        # Fill in the rest with default values
        return self.fillOLAP(o)
        
    def fromObject(self, obj):
        # Create an OLAP object from another query-able object
        
        # Find the type of object and 
        # get a result to do some guessing on the data types
        
        filters = []
        
        f = Filter()
        inspKeys, measKeys = f.keyNamesHash()
        
        if type(obj) == Measurement:
            filterKeys = measKeys
            filterType = 'measurement'
        elif type(obj) == Inspection:
            filterKeys = inspKeys
            filterType = 'framefeature'
        else:
            log.warn('OLAP factory got unknown type %s' % str(type(obj)))
            filterKeys = []
            filterType = 'unknown'
            
        for key in filterKeys:
            filters.add({'type': filterType, 'name': obj.name + '.' + key, 'exists':1})
        
        
        # Put together the OLAP
        o = OLAP()
        o.olapFilter = filters
        
        # Fill in the rest with default values
        return self.fillOLAP(o)
        
    
    def fillOLAP(self, o):
        # Fills in default values for undefined fields of an OLAP
        
        if o.olapFilter:
            o.name = o.olapFilter[0]['name'] + '_' + str(randint(1, 1000000))
        else:
            o.name = 'GeneratedOLAP_' + str(randint(1, 1000000))
            
        # Default to max query length of 1000
        if not o.maxLen:
            o.maxLen = 1000
            
        # No mapping of output values
        if not o.valueMap:
            o.valueMap = []
    
        # No since constratint
        if not o.since:
            o.since = None
        
        # No before constraint
        if not o.before:
            o.before = None
            
        # Finally, run once to see if need to aggregate
        if not o.statsInfo:
            results = o.execute()
            
            # If to long, do the aggregation
            if len(results) > o.maxLen:
                self.autoAggregate(results, autoUpdate=False)
            
        # Return the result
        # NOTE: This OLAP is not saved 
        return o
        

    
class RealtimeOLAP():
    
    def realtime(self, frame):
        
        charts = Chart.objects()
        
        # Functions below assume frame is a dict not an object 
        #frame = frame.__dict__['_data']
        
        for chart in charts:
            # If no statistics, send result on its way
            # If there are stats, it will be handled later by stats scheduler
            olap = OLAP.objects(name=chart.olap)[0]
            if not olap.statsInfo:
                filters = olap.olapFilter
                olapOK = True
                
                i = 0
                while olapOK and i < len(filters):
                    f = filters[i]
                    i += 1
                    
                    if f['type'] == 'measurement':
                        name, dot, field = f['name'].partition('.')
                        f['name'] = field
                        part = False
                        for r in frame['results']:
                            part = part or (r['py/state']['measurement_name'] == name and self.checkFilter(f, r['py/state']))
                        olapOK = part
                    elif f['type'] == 'framefeature':
                        name, dot, field = f['name'].partition('.')
                        f['name'] = field
                        part = False
                        for fe in frame['features']:
                            part = part or (fe['py/state']['featuretype'] == name and self.checkFilter(f, fe['py/state']))
                        olapOK = part
                    else:
                        olapOK = self.checkFilter(f, frame)
                
                if olapOK:
                    data = frame.copy()
                    f = Filter()
                    data = f.unEmbed(data)
                    data = f.flattenFrame([data])
                    data = pd.DataFrame(data)
                    data = olap.doPostProc(data)
                    data = [v for v in data.transpose().to_dict().values()]
                    data = chart.mapData(data)
                    self.sendMessage(chart, data)
                
    def checkFilter(self, filt, frame):
        keyParts = filt['name'].split('.')
        value = self.getFrameField(frame, keyParts)
        
        if 'exists' in filt and value:
            return True
        elif 'eq' in filt:
            return filt['eq'] == value
        elif 'gt' in filt or 'lt' in filt:
            part = True
            if 'gt' in filt:
                part = part and filt['gt'] == value
            if 'lt' in filt:
                part = part and filt['lt'] == value
            return part
        else:
            log.info('Unknown realtime filter parameter')
            return True
        
    def getFrameField(self, field, keyParts):
        # This function recursively pulls apart the key parts to unpack the hashes and find the actual value
        
        # Need special handling for results and features
        if keyParts[0] == 'results' or keyParts[0] == 'features':
            embeddedDocs = field[keyParts.pop()]
            docBool = False
            for d in embeddedDocs:
                # work off copy to preserve original for future iterations of loop
                keys = keyParts[:]
                docBool = docBool or self.getFrameField(d._data, keys)
        
        if len(keyParts) == 1:
            return field.get(keyParts[0], None)
        else:
            return self.getFrameField(field.get(keyParts.pop(0), {}), keyParts) 
                
    def sendMessage(self, chart, data):
        
        if (len(data) > 0):
            msgdata = dict(
                chart = str(chart.name),
                data = data)
            
            chartName = 'Chart/%s/' % utf8convert(chart.name) 
            ChannelManager().publish(chartName, dict(u='data', m=msgdata))
    
    def monitorRealtime(self):
        from .base import jsondecode
        
        cm = ChannelManager()
        sock = cm.subscribe('frame/')
        
        while True:
            cname = sock.recv()
            frame = jsondecode(sock.recv())
            self.realtime(frame)
            

class ScheduledOLAP():
    
    def runSked(self):
        
        log.info('Starting statistics schedules')
        
        glets = []
        
        glets.append(Greenlet(self.skedLoop, 'minute'))
        glets.append(Greenlet(self.skedLoop, 'hour'))
        glets.append(Greenlet(self.skedLoop, 'day'))
        
        glets.append(Greenlet(self.checkTransient))
        
        # Start all the greenlets
        for g in glets:
            g.start()
            
        # Join all the greenlets
        for g in glets:
            g.join()
    
    def checkTransient(self):
        while True:
            # First delete transients that are inactive
            olds = OLAP.objects(transient = True, confirmed = False)
            for o in olds:
                c = Chart.objects(olap=o.name)[0]
                c.delete()
                o.delete()
                log.info('Deleted transient OLAP: %s' % o.name)
                log.info('Deleted associated chart: %s' % c.name)
        
                
            # Request updates on status of active olaps
            active = OLAP.objects(transient = True)
            for a in active:
                # Set status to inactive until a client responds that it is in use
                a.confirmed = False
                a.save()
                
                # Publish a request that any listening clients confirm they are listening
                chart = Chart.objects(olap=a.name)[0]
                chartName = 'Chart/%s/' % utf8convert(chart.name) 
                ChannelManager().publish(chartName, dict(u='data', m='ping'))
            
            sleep(3600)
            
    def skedLoop(self, interval):
        
        from datetime import datetime
        from .models.Chart import Chart
        
        nextTime = datetime.utcnow()
        
        while (True):
            log.info('Checking for OLAPs running every %s' % interval)
            
            # Split the time into components to make it easier to round
            year = nextTime.year
            month = nextTime.month
            day = nextTime.day
            hour = nextTime.hour
            minute = nextTime.minute
            
            # Setup the start and end time for the intervals
            if interval == 'minute':
                endBlock = datetime(year, month, day, hour, minute)
                startBlock = endBlock - timedelta(0, 60)
                nextTime = endBlock + timedelta(0, 61)
                sleepTime = 60
            elif interval == 'hour':
                endBlock = datetime(year, month, day, hour, 0)
                startBlock = endBlock - timedelta(0, 3600)
                nextTime = endBlock + timedelta(0, 3661)
                sleepTime = 3600
            elif interval == 'day':
                endBlock = datetime(year, month, day, 0, 0)
                startBlock = endBlock - timedelta(1, 0)
                nextTime = endBlock + timedelta(1, 1)
                sleepTime = 86400
            
            # OLAPs assume time in epoch seconds
            startBlockEpoch = mktime(startBlock.timetuple())
            endBlockEpoch = mktime(endBlock.timetuple())

            # Find all OLAPs that run on this time interval
            os = OLAP.objects(groupTime = interval) 
    
            # Have each OLAP send the message
            for o in os:
                
                log.info('%s running per %s' % (o.name, interval)) 
                
                o.since = startBlockEpoch
                o.before = endBlockEpoch
                data = o.execute()
                
                cs = Chart.objects(olap = o.name)
                    
                for c in cs:
                    chartData = c.mapData(data)
                    ro = RealtimeOLAP() # To get access to the sendMessage fn
                    ro.sendMessage(o, chartData, c.name)
                
                
            # Set the beginning time interval for the next iteraction
            sleepTime = (nextTime - datetime.utcnow()).total_seconds()
            
            # Wait until time to update again
            sleep(sleepTime)
