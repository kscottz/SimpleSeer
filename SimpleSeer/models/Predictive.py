from SimpleSeer.Filter import Filter

import logging

import mongoengine
from .base import SimpleDoc

import pandas as pd

log = logging.getLogger(__name__)

class Predictive(SimpleDoc, mongoengine.Document):
    
    filterFields = mongoengine.DictField()
    dependent = mongoengine.StringField()
    independent = mongoengine.ListField()
    method = mongoengine.StringField()
    betas = mongoengine.ListField()
    
    def getData(self):
        f = Filter()
        tot, frames = f.getFrames(self.filterFields)
        df = pd.DataFrame(f.flattenFeature(frames))
        
        deps = df[self.dependent]
        inds = pd.DataFrame({var:df[var] for var in self.independent})
		    		
        return inds, deps
        
    def transformData(self, inds):
        from calendar import timegm
        
        for field in inds:
            if inds[field].dtype == str:
                # Turn string variables into categorical variables
                for element in inds[field].unique():
                    inds[i] = field == i 
            if inds[field].dtype == object:
                # Assume objects are datetimes, which need to be converted to epoch seconds
                inds[field] = inds[field].apply(lambda x: timegm(x.timetuple()))
        
        return inds
                
    def estimate(self, deps, inds):
        from numpy import dot, linalg
        
        model = pd.ols(y = deps, x = inds)
        
        return model.betas.values
        
    def execute(self, frame):
        
        tally = 0
        for beta, field in zip(self.betas, self.indendent):
            tally += frame[field] * beta
            
        return tally
    
    def update(self):
        
        inds, deps = self.getData()
        inds = self.transformData(inds)
        self.betas = self.estimate(deps, inds)
        self.save()

    def partial(self, deps, inds, partial):
        from numpy import dot, linalg
        
        tmp = {}
        for var in inds:
            if var != partial: temp[var] = inds[var]
            
        sInds = pd.DataFrame({var:df[var] for var in self.independent})
        model1 = pd.ols(y = deps, x = sInds)
        model2 = pd.ols(y = inds[partial], x = sInds)
        
        return model1.resid, model2.resid

class NelsonRules:
    # Note as True value indicates failure
    
    @classmethod
    def rule1(self, points, mean, sd):
        # If a point is more than 3 standard deviations from mean
        upper = mean + (3 * sd)
        lower = mean - (3 * sd)
        for p in points:
            if p > upper or p < lower:
                return True
        return False
    
    @classmethod    
    def rule2(self, points, mean):
        # if 9 or more points in a row lie on same side of mean
        aboveBelow = 0
        run = 0
        
        for p in points:
            if p > mean:
                if aboveBelow != 1:
                    # if change from below to above mean, reset run counter
                    run = 0
                aboveBelow = 1
            if p < mean:
                if aboveBelow != -1:
                    # if change from above to below mean, reset run counter
                    run = 0
                aboveBelow = -1
            run += 1
            
            if run == 9:
                return True
    
        return False    
        
    @classmethod
    def rule3(self, points):
        # 6 or more increasing or decreasing
        
        last = points.pop(0)
        gtOrLt = 0
        run = 0
        
        for p in points:
            if p > last:
                if gtOrLt != 1:
                    run = 0
                gtOrLt = 1
            if p < last:
                if gtOrLt != -1:
                    run = 0
                gtOrLt = -1
            run += 1
            if run == 5:
                return True
        
            last = p
        
        return False
            
    @classmethod
    def rule4(self, points):
        # 14 or more points alternating
        last = points.pop(0)
        gtOrLt = 0
        run = 0
        
        for p in points:
            if p > last:
                if gtOrLt != -1:
                    run = 0
                gtOrLt = 1
            if p < last:
                if gtOrLt != 1:
                    run = 0
                gtOrLt = -1
            
            run += 1
            if run == 13:
                return True
                
            last = p
            
        return False

    @classmethod
    def rule5(self, points, mean, sd):
        # two out of three points in a row more than 2 standard devs from mean
        
        if len(points) < 2:
            return False
        
        twoBack = points.pop(0)
        threeBack = points.pop(0)
        
        upper = mean + (2 * sd)
        lower = mean - (2 * sd)
        
        for p in points:
            numOut = 0
            if threeBack > upper or threeBack < lower:
                numOut += 1 
            if twoBack > upper or twoBack < lower:
                numOut += 1
            if p > upper or p < lower:
                numOut += 1
                
            if numOut >= 2:
                return True
        
            threeBack = twoBack
            twoBack = p
        
        return False
        
    @classmethod
    def rule6(self, points, mean, sd):
        # four out of five points in a row more than 1 standard dev from mean
        if len(points) < 5:
            return False
        
        upper = mean + sd
        lower = mean - sd
        
        for i in range(len(points)-4):
            above = 0
            for x in range(i,i+5):
                if points[x] > upper or points[x] < lower:
                    above += 1
            
            if above >= 4:
                return True
            
        return False
            
    @classmethod
    def rule7(self, points, mean, sd):
        # 15 or more points in a row within 1 standard dev from mean
        if len(points) < 15:
            return False
        
        upper = mean + sd
        lower = mean - sd
        run = 0
            
        for i in range(len(points)):
            if points[i] > upper or points[i] < lower:
                run = 0
            
            run += 1
            
            if run == 15:
                return True
        
        return False
        
    @classmethod
    def rule8(self, points, mean, sd):
        # 8 points in a row with none within a standard deviation of mean
        if len(points) < 8:
            return False
        
        upper = mean + sd
        lower = mean - sd
        
        run = 0
        
        for i in range(len(points)):
            if points[i] < upper and points[i] > lower:
                run = 0
            
            run += 1
            
            if run == 8:
                return True
        
        return False
        
