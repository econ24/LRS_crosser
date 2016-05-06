# -*- coding: utf-8 -*-
"""
Created on Fri May  6 12:16:47 2016

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

maxDistance = 4
lengthThreshold = 0.5

crossSql = '''
SELECT "ROUTE_ID" AS route_id, ST_Transform(geom, 2163), feat_id 
FROM hpms_ny_2013 AS hpms
JOIN npmrds_shapefile AS npmrds
ON ST_Distance(ST_Transform(wkb_geometry, 2163), 
    ST_Transform(geom, 2163)) <= %s
WHERE link_id = %s;
'''

insertSql = '''
INSERT INTO hpms_lut
VALUES (%s, %s, %s);
'''

class HpmsThread(threading.Thread):

    pgConnection = None
    linkIdList = []
    listLock = None
    
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.workToDo = True
        self.pgCursor = self.getCursor()
        self.name = "thread-" + str(name)
        
    def getCursor(self):
        if HpmsThread.pgConnection:
            return HpmsThread.pgConnection.cursor()
            
        return None
        
    def run(self):
        if not self.pgCursor:
            return
            
        while self.workToDo:
            
            linkId = None
            
            HpmsThread.listLock.acquire()
            
            if len(HpmsThread.linkIdList):
                linkId = HpmsThread.linkIdList.pop()
            
            self.workToDo = bool(len(HpmsThread.linkIdList))
            
            HpmsThread.listLock.release()
            
            if linkId:
                self.processLinkId(linkId)
        # end while

        self.pgCursor.close()
            
    def processLinkId(self, linkId):
        linkGeom = self.getLinkGeometry(linkId)
        minLength = linkGeom.length * lengthThreshold
        
        linkVector, linkBuffer = self.getLinkData(linkGeom)
        
        npmrdsFeatIds, hpmsGeometries = self.getHpmsDicts(linkId)
        
        intersections = self.getIntersections(linkBuffer, hpmsGeometries)
            
        vectors = self.getVectors(intersections)
                
        finalResults = [ (linkId, key, npmrdsFeatIds[key]) \
            for key, val in vectors.items() \
            if math.fabs(linkVector.dotProduct(val)) >= 0.9 \
            and intersections[key].length >= minLength ]
            
        self.pgCursor.executemany(insertSql, finalResults)
        HpmsThread.pgConnection.commit()
        
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
        linkBuffer = linkGeom.envelope.buffer(maxDistance)
        
        return linkVector, linkBuffer
        
    def getHpmsDicts(self, linkId):
        hpmsGeometries = {}
        npmrdsFeatIds = {}
        
        self.pgCursor.execute(crossSql, [maxDistance, linkId])
        
        for item in self.pgCursor:
            key = item[0]
            npmrdsFeatIds[key] = item[2]
            hpmsGeometries[key] = wkb.loads(item[1], hex=True)
            
        return npmrdsFeatIds, hpmsGeometries
        
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

def initHpmsThread(pgConn, linkList):
    HpmsThread.pgConnection = pgConn
    HpmsThread.linkIdList = linkList
    HpmsThread.listLock = threading.Lock()