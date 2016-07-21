# -*- coding: utf-8 -*-
"""
Created on Fri May  6 12:08:23 2016

@author: econ24
"""

import psycopg2, json, sys
from HpmsThread import HpmsThread, initHpmsThread
from HpmsThreadSecondPass import HpmsSecondPass, initHpmsSecondPass
from ThreadChecker import ThreadChecker

def main():
    if len(sys.argv) < 2:
        print '''
        Usage: HPMS_x_NPMRDS.py <fips> [threads] [-sp]
        
        fips: 5 digit (2 state + 3 county) fips code
        threads: number of threads process will spawn, defaults to 4
        -sp: runs the HPMS second pass
        '''
        return
        
    numThreads = 4
    secondPass = False
    if (len(sys.argv) > 2):
        arg = sys.argv[2]
        if arg == '-sp':
            secondPass = True
        else:
            numThreads = int(sys.argv[2])
    if (len(sys.argv) > 3):
        arg = sys.argv[3]
        if arg == '-sp':
            secondPass = True
        else:
            numThreads = int(sys.argv[3])
        
    connectionData = None
    try:
        connectionData = getConnectionData()
    except Exception as e:
        print "There was an exception while attempting to read file: 'connectionData.json"
        print "Exception:", e
    
    if not connectionData:
        print "Connection data undefined!"
        print "Exiting..."
        return
    
    connection = None
    try:
        connection = psycopg2.connect(**connectionData)
    except Exception as e:
        print "There was an exception while attempting connection to database"
        print "Exception:", e
    
    if not connection:
        print "Connection to database not established!"
        print "Exiting..."
        return
        
    cursor = connection.cursor()
    
    fips = str(sys.argv[1])
    
    linkIds = getCountyLinks(cursor, fips)
    
    cursor.close()
    
    if secondPass:
        initHpmsSecondPass(connection, linkIds)
        threads = [ HpmsSecondPass(x) for x in range(numThreads) ]
        threadChecker = ThreadChecker(threads, HpmsSecondPass.linkIdList)
    else:
        initHpmsThread(connection, linkIds)
        threads = [ HpmsThread(x) for x in range(numThreads) ]
        threadChecker = ThreadChecker(threads, HpmsThread.linkIdList)
        
    
    for thread in threads:
        thread.start()
        
    threadChecker.start()
    threadChecker.join()
        
    for thread in threads:        
        thread.join()
        
    connection.commit()
    connection.close()
    
def getConnectionData():
    with open("./connectionData.json") as jsonFile:
        return json.load(jsonFile, encoding="utf-8")
    
def getCountyLinks(cursor, fips):
    sql = '''
        SELECT link_id 
        FROM npmrds_shapefile AS npmrds 
        JOIN tl_2013_us_county AS bounds 
        ON ST_Intersects(bounds.the_geom, npmrds.wkb_geometry) 
        WHERE statefp = %s AND countyfp = %s 
        AND link_id NOT IN (SELECT DISTINCT link_id FROM hpms_lut)
    '''
    cursor.execute(sql, [ fips[0:2], fips[2:] ])
    return [ int(result[0]) for result in cursor ]
    
def getRoadLinks(cursor):
    sql = '''
        SELECT link_id
        FROM npmrds_shapefile
        WHERE st_name LIKE 'WOLF RD'
        AND link_id NOT IN (SELECT DISTINCT link_id FROM hpms_lut)
    '''
    cursor.execute(sql)
    return [ int(result[0]) for result in cursor ]

if __name__ == "__main__":
    main()