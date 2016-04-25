import psycopg2, json, sys
from LrsThread import LrsThread, initLrsThread

def main():
    if len(sys.argv) != 2:
        print '''
        "Usage: LRS_test.py <5 digit (state + county) fips code>
        '''
        return
        
    connectionData = None
    try:
        connectionData = getConnectionData()
    except Exception as e:
        print "There was an exception while attempting to read file: 'connectionData.json'"
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
    
    initLrsThread(connection, linkIds)
    
    threads = [ LrsThread(x) for x in range(4) ]
    
    for thread in threads:
        thread.start()
        
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
        ON statefp = %s AND countyfp = %s 
        WHERE ST_Intersects(bounds.the_geom, npmrds.wkb_geometry)
    '''
    cursor.execute(sql, [ fips[0:2], fips[2:] ])
    linkIds = [ int(result[0]) for result in cursor ]
    
    sql = '''
        SELECT DISTINCT link_id 
        FROM lrs_lut
    '''
    cursor.execute(sql)
    linkSet = set([ int(result[0]) for result in cursor ])
    
    return [ linkId for linkId in linkIds if linkId not in linkSet ]

if __name__ == "__main__":
    main()