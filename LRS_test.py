import psycopg2
from LrsThread import LrsThread, initLrsThread

connectionData = {
    "host": "lor.availabs.org",
    "user": "postgres",
    "password": "transit",
    "database": "NHS_NPMRDS"
}

def main():
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
    
def getLinkIds(cursor):
    sql = '''
        SELECT link_id
        FROM npmrds_shapefile
        WHERE st_name LIKE 'WOLF RD'
        '''
    cursor.execute(sql)
    return [ result[0] for result in cursor ]

if __name__ == "__main__":
    main()