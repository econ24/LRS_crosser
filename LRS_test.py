import psycopg2, json
from LrsThread import LrsThread, initLrsThread
from LrsChecker import LrsChecker

linkIds = [91893691, 91894354, 811143677, 811143678, 908458956, 926739527, 926739528, 91909046, 91909047, 927082491, 927082492, 927082493, 927082494, 927094375, 927094376, 927094377, 927094378, 927094379, 91909042, 91910291, 91913932, 743327418, 743330207, 743330208, 926837817, 926837818, 926837819, 926837820, 926837824, 926837825, 926837826, 926837827, 926837828, 943937735, 943937737, 943937738, 945720485]
#linkIds = [91924188]

def main():
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
    
    #linkIds = getLinkIds(cursor)
    
    cursor.close()
    
    initLrsThread(connection, linkIds)
    
    threads = [ LrsThread(x) for x in range(4) ]
        
    lrsChecker = LrsChecker(len(threads))
    
    for thread in threads:
        thread.start()
        
    lrsChecker.start()
    lrsChecker.join()
        
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