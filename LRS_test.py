import psycopg2, json
from LrsThread import LrsThread, initLrsThread

def main():
    connectionData = None
    try:
        connectionData = getConnectionData()
    except:
        pass
    
    if not connectionData:
        print "Could not read file: 'connectionData.json'"
        print "Exiting..."
        return
    
    connection = psycopg2.connect(**connectionData)
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