from .models.OLAP import OLAP
from .models.Measurement import Measurement
from .models.Inspection import Inspection

from .Filter import Filter

from gevent import Greenlet, sleep
from datetime import datetime, timedelta
from time import mktime

from .util import utf8convert
from .realtime import ChannelManager

import pandas as pd

import logging
log = logging.getLogger(__name__)


class OLAPFactory:
    
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
        from random import randint
        # Fills in default values for undefined fields of an OLAP
        
        o.name = o.olapFilter[0]['name'] + '_' + str(randint(1, 1000000))
            
        # Default to max query length of 1000
        if not o.maxLen:
            o.maxLen = 1000
            
        # No mapping of output values
        if not o.valueMap:
            o.valueMap = {}
    
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
        
        conds = []
        for res in frame.results:
            conds.append({'queryType': 'measurement_id', 'queryId': res.measurement_id})
        for feat in frame.features:
            conds.append({'queryType':'inspection_id', 'queryId': feat.inspection})
        
        f = Filter()
        f2 = f.flattenFrame([frame])
            
        olaps = OLAP.objects(__raw__={'$or': conds}) 
        
        if olaps:
            f = Filter()
            frame = f.flattenFrame([frame])
            for o in olaps:
                # If no statistics, send result on its way
                # If there are stats, it will be handled later by stats scheduler
                if not o.statsInfo:
                    oFrame = self.formatFrame(o, frame)
                    dFrame = [v for v in oFrame.transpose().to_dict().values()][0]
                    if len(dFrame):
                        self.sendOLAP(dFrame, o)

                 
    def sendOLAP(self, data, o):
        from .models.Chart import Chart
        
        if len(data) > 0:
            # Need long term fix: only publish to charts that are listened to
            cs = Chart.objects(olap = o.name)
            
            for c in cs:
                thisData = data.copy()
                chartData = c.mapData([thisData])
                self.sendMessage(o, chartData, c.name)                     


    def formatFrame(self, o, frame):
        
        sinceok = (not o.since) or (frame.capturetime > o.since)
        beforeok = (not o.before) or (frame.capturetime < o.before)
        
        if sinceok and beforeok:
            # Use only the specified fields
            frame = pd.DataFrame(frame)
            frame = o.doPostProc(frame)
        
        return frame


    def sendMessage(self, o, data, subname):
        if (len(data) > 0):
            msgdata = dict(
                olap = str(o.name),
                data = data)
        
            olapName = 'Chart/%s/' % utf8convert(subname) 
            ChannelManager().publish(olapName, dict(u='data', m=msgdata))
            

class ScheduledOLAP():
    
    def runSked(self):
        
        log.info('Starting statistics schedules')
        
        glets = []
        
        glets.append(Greenlet(self.skedLoop, 'minute'))
        glets.append(Greenlet(self.skedLoop, 'hour'))
        glets.append(Greenlet(self.skedLoop, 'day'))
    
        # Start all the greenlets
        for g in glets:
            g.start()
            
        # Join all the greenlets
        for g in glets:
            g.join()
        
        
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
