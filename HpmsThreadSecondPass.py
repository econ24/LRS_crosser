# -*- coding: utf-8 -*-
"""
Created on Wed May 18 16:00:21 2016

@author: econ24
"""

import threading, math
from shapely import wkb

from Vector2d import Vector2d
    
linkSql = '''
SELECT ST_Transform(wkb_geometry, 2163)
FROM npmrds_shapefile
WHERE link_id = %s;
'''

distanceThreshhold = 20     # meters
lengthThreshold = 0.5       # as a percent of comparison link length
dotProductThreshold = 0.98  # for angle detection

crossSql = '''
SELECT DISTINCT hpms.id AS hpms_id, route_id, feat_id, ST_Transform(geom, 2163)
FROM hpms_newyork_2013 AS hpms
JOIN npmrds_shapefile AS npmrds 
ON ST_Distance(ST_Transform(wkb_geometry, 2163), 
    ST_Transform(geom, 2163)) <= %s 
WHERE link_id = %s 
AND hpms.id IN (SELECT DISTINCT hpms_id FROM hpms_lut);
'''

insertSql = '''
INSERT INTO hpms_lut
VALUES (%s, %s, %s, %s);
'''

class HpmsSecondPass(threading.Thread):

    pgConnection = None
    linkIdList = []
    listLock = None
    
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.workToDo = True
        self.pgCursor = self.getCursor()
        self.name = "thread-" + str(name)
        
    def getCursor(self):
        if HpmsSecondPass.pgConnection:
            return HpmsSecondPass.pgConnection.cursor()
            
        return None
        
    def run(self):
        if not self.pgCursor:
            return
            
        while self.workToDo:
            
            linkId = None
            
            HpmsSecondPass.listLock.acquire()
            
            if len(HpmsSecondPass.linkIdList):
                linkId = HpmsSecondPass.linkIdList.pop()
            
            self.workToDo = bool(len(HpmsSecondPass.linkIdList))
            
            HpmsSecondPass.listLock.release()
            
            if linkId:
                self.processLinkId(linkId)
        # end while

        self.pgCursor.close()
            
    def processLinkId(self, linkId):
        linkGeom = self.getLinkGeometry(linkId)
        minLength = linkGeom.length * lengthThreshold
        
        linkVector, linkBuffer = self.getLinkData(linkGeom)
        
        hpmsRouteIds, npmrdsFeatIds, hpmsGeometries = self.getHpmsDicts(linkId)
        
        intersections = self.getIntersections(linkBuffer, hpmsGeometries)
            
        vectors = self.getVectors(intersections)
                
        finalResults = [ (linkId, key, hpmsRouteIds[key], npmrdsFeatIds[key]) \
            for key, val in vectors.items() \
            if math.fabs(linkVector.dotProduct(val)) >= dotProductThreshold \
            and intersections[key].length >= minLength ]
            
        self.pgCursor.executemany(insertSql, finalResults)
        HpmsSecondPass.pgConnection.commit()
        
    def getLinkGeometry(self, linkId):
        self.pgCursor.execute(linkSql, [linkId])
        result = self.pgCursor.fetchone()
        
        linkGeom = wkb.loads(result[0], hex=True)
        
        return linkGeom
        
    def getLinkData(self, linkGeom):
        coords = linkGeom.coords
        start = coords[0]
        end = coords[-1]
        linkVector = Vector2d(end[0]-start[0], end[1]-start[1]).normalize()
        linkBuffer = linkGeom.envelope.buffer(distanceThreshhold)
        
        return linkVector, linkBuffer
        
    def getHpmsDicts(self, linkId):
        hpmsGeometries = {}
        hpmsRouteIds = {}
        npmrdsFeatIds = {}
        
        # hpms_id, route_id, feat_id, ST_Transform(geom, 2163)
        self.pgCursor.execute(crossSql, [distanceThreshhold, linkId])
        
        for item in self.pgCursor:
            key = item[0]
            hpmsRouteIds[key] = item[1]
            npmrdsFeatIds[key] = item[2]
            hpmsGeometries[key] = wkb.loads(item[3], hex=True)
            
        return hpmsRouteIds, npmrdsFeatIds, hpmsGeometries
        
    def getIntersections(self, linkBuffer, hpmsGeometries):
        return { key: val.intersection(linkBuffer) \
            for key, val in hpmsGeometries.items() }
        
    def getVectors(self, intersections):
        vectors = {}
            
        for key, val in intersections.items():
            try:
                coords = val.coords
                start = coords[0]
                end = coords[-1]
                vectors[key] = Vector2d(end[0]-start[0], \
                    end[1]-start[1]).normalize()
            except NotImplementedError:
                pass
                
        return vectors

def initHpmsSecondPass(pgConn, linkList):
    HpmsSecondPass.pgConnection = pgConn
    HpmsSecondPass.linkIdList = linkList
    HpmsSecondPass.listLock = threading.Lock()