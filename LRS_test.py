import psycopg2, json
from LrsThread import LrsThread, initLrsThread

def main():
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
    
    linkIds = getLinkIds(cursor)
    
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
    
def getLinkIds(cursor):
    sql = '''
        SELECT link_id
        FROM npmrds_shapefile
        WHERE st_name LIKE 'WOLF RD'
        '''
    cursor.execute(sql)
    return [ int(result[0]) for result in cursor ]

if __name__ == "__main__":
    main()