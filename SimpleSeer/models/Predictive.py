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
