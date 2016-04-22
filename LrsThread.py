# -*- coding: utf-8 -*-
"""
Created on Thu Apr 21 15:14:48 2016

@author: econ24
"""

import threading, math
from shapely import wkb

from Vector2d import Vector2d
    
linkSql = '''
SELECT ST_Transform(wkb_geometry, 2163), dir_travel
FROM npmrds_shapefile
WHERE link_id = %s;
'''

maxDistance = 4
crossSql = '''
SELECT objectid, ST_Transform(geom, 2163), direction
FROM "LRS" AS hpms
JOIN npmrds_shapefile AS npmrds
ON ST_Distance(ST_Transform(wkb_geometry, 2163), 
    ST_Transform(geom, 2163)) <= %s
WHERE link_id = %s;
'''

insertSql = '''
INSERT INTO lrs_lut
VALUES (%s, %s, %s, %s);
'''

class LrsThread(threading.Thread):

    pgConnection = None
    linkIdList = []
    listLock = None
    
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.workToDo = True
        self.pgCursor = self.getCursor()
        self.name = "thread-" + str(name)
        
    def getCursor(self):
        if LrsThread.pgConnection:
            return LrsThread.pgConnection.cursor()
            
        return None
        
    def run(self):
        if not self.pgCursor:
            return
            
        while self.workToDo:
            
            linkId = None
            
            LrsThread.listLock.acquire()
            
            if len(LrsThread.linkIdList):
                linkId = LrsThread.linkIdList.pop()
            
            self.workToDo = bool(len(LrsThread.linkIdList))
            
            LrsThread.listLock.release()
            
            if linkId:
                self.processLinkId(linkId)
        # end while

        self.pgCursor.close()
            
    def processLinkId(self, linkId):
        linkGeom, linkDirection = self.getLinkGeometry(linkId)
        minLength = linkGeom.length * 0.75
        
        linkVector, linkBuffer = self.getLinkData(linkGeom)
        
        lrsResults, lrsGeometries = self.getLrsDicts(linkId)
        
        intersections = self.getIntersections(linkBuffer, lrsGeometries)
            
        vectors = self.getVectors(intersections)
                
        finalResults = [ (linkId, key, linkDirection, lrsResults[key]) \
            for key, val in vectors.items() \
            if math.fabs(linkVector.dotProduct(val)) >= 0.9 \
            and intersections[key].length >= minLength ]
            
        self.pgCursor.executemany(insertSql, finalResults)
        LrsThread.pgConnection.commit()
        
    def getLinkGeometry(self, linkId):
        self.pgCursor.execute(linkSql, [linkId])
        result = self.pgCursor.fetchone()
        
        linkGeom = wkb.loads(result[0], hex=True)
        linkDirection = result[1]
        
        return linkGeom, linkDirection
        
    def getLinkData(self, linkGeom):
        coords = linkGeom.coords
        start = coords[0]
        end = coords[-1]
        linkVector = Vector2d(end[0]-start[0], end[1]-start[1]).normalize()
        linkBuffer = linkGeom.envelope.buffer(maxDistance)
        
        return linkVector, linkBuffer
        
    def getLrsDicts(self, linkId):
        lrsResults = {}
        lrsGeometries = {}
        
        self.pgCursor.execute(crossSql, [maxDistance, linkId])
        
        for item in self.pgCursor:
            key = item[0]
            lrsResults[key] = item[2]
            lrsGeometries[key] = wkb.loads(item[1], hex=True)
            
        return lrsResults, lrsGeometries
        
    def getIntersections(self, linkBuffer, lrsGeometries):
        return { key: val.intersection(linkBuffer) \
            for key, val in lrsGeometries.items() }
        
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

def initLrsThread(pgConn, linkList):
    LrsThread.pgConnection = pgConn
    LrsThread.linkIdList = linkList
    LrsThread.listLock = threading.Lock()