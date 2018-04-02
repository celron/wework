import urllib3
import json
import itertools
import certifi
import MySQLdb
#import PySQLPool
from flask import Flask
from retryConnect import RetryConnect 
import os


app = Flask(__name__)

app.config.from_pyfile('settings.cfg')
environ_check = os.environ.get('SETTINGS','settings.cfg')
#print app.config
print 'environ_check',environ_check
config = app.config['HOME_DIR']
#print 'config', config
#environ_check = readConfig('%s/settings.ini' % config)


def connect(app1):
    #print 'wework connect',app1
    return RetryConnect('wework', app1['SERVER'], app1['USERID'], app1['PASSWORD'], 'loolmapsprod')

# urllib3.contrib.pyopenssl.inject_into_urllib3()

http = urllib3.PoolManager( cert_reqs='CERT_REQUIRED',   ca_certs=certifi.where())

#connection = PySQLPool.getNewConnection(username=app.config['USERID'], password=app.config['PASSWORD'], host=app.config['SERVER'], use_unicode=True,charset='utf8' )

def readGeoGroupings():
    #response = urllib2.urlopen('https://api-proxy.wework.com/locations/api/v1/geogroupings')
    #a = response.read()
    #return json.loads(a)
    string = 'https://api-proxy.wework.com/locations/api/v1/geogroupings'
    return json.loads(urlread(string))

def readGeoGroup(group):
    #response = urllib3.urlopen('https://api-proxy.wework.com/locations/api/v1/geogroupings/'+group)
    string = 'https://api-proxy.wework.com/locations/api/v1/geogroupings/'
    a = response.read()
    return json.loads(a)


def readBuilding(building):
    response = urllib3.urlopen('https://api-proxy.wework.com/locations/api/v2/buildings/'+building)
    a = response.read()
    return json.loads(a)

def urlread(url):
    r = http.request('GET',url)
    print r.status
    data = r.data
    return data

class WeworkFeeds():
    geogroups_data = []
    subgeogroup_data = []
    building_list = []
    building_data = []

# this will read the geoGroupings
    def processGeoGroupings(self):
        a = readGeoGroupings()
        self.geogroups_data = a['geogroupings']
        return len(self.geogroups_data)

    def processSubGeogroup(self):
        self.subgeogroup_data = []
        for group in self.geogroups_data:
            slug = group['slug']
            subgroup = readGeoGroup(slug)
            self.subgeogroup_data.append(subgroup['geogrouping'])
        return len(self.subgeogroup_data)

    # get the building list and building slug
    def generate_building_list(self):
        for group in self.subgeogroup_data:
            for building in group['buildings']:
                slug = building['slug']
                name = building['name']
                self.building_list.append({'name': name, 'slug':slug})
        self.building_list.sort()
        a = itertools.groupby(self.building_list)
        self.building_list = list(k for k,_ in a)
        return len(self.building_list)


    #this will load each building, may take a long time
    def processBuildings(self):
        for building in self.building_list:
            building_info = readBuilding(building['slug'])
            print 'building:',building['slug']
            self.building_data.append(building_info['building'])
        return len(self.building_data)

    def loadBasicData(self):
        self.processGeoGroupings()
        self.processSubGeogroup()
        self.generate_building_list()


def checkWeworkId(id):
    sql  = 'SELECT count(*) FROM wework_transfer WHERE wework_id = "%s"'
    cursor_string = sql % id
    # print cursor_string
    results = conn.execute(cursor_string)
    results = conn.fetchall()
    return results

def insertWeworkEntry(data):
    sql = 'INSERT INTO wework_transfer(name, wework_id, lat, lng) VALUES ("%s", "%s", %f, %f);'
    #query = PySQLPool.getNewQuery(connection)
    cursor_string = sql %(data['name'], data['id'],float(data['latitude']),float(data['longitude']))
    #query.Query(sql, (data['name'],data['id'],float(data['latitude']),float(data['longitude'])))
    #PySQLPool.commitPool()
    #cursor_string = sql % (data['name'],data['id'],data['latitude'],data['longitude'])
    #results = execute(cursor_string)
    results = conn.execute(cursor_string)
    conn.commit()
    print '%s %s' %(cursor_string,results)
    return results
     
def searchWeworkTransfer():
    sql = 'SELECT name,id,wework_id,location_id FROM wework_transfer WHERE location_id is NULL'
    conn.execute(sql)
    return conn.fetchall()

def searchLocation(data):
    sql = 'SELECT id,name FROM locations WHERE name LIKE "WEWORK %s"'
    cursor_string = sql %(data.upper())
    # print cursor_string
    conn.execute(cursor_string)
    return conn.fetchall()

def updateWeworkTransfer(id,location_id):
    sql = 'UPDATE wework_transfer SET location_id=%s WHERE id=%d'
    cursor_string = sql %(location_id,id)
    print cursor_string
    conn.execute(cursor_string)
    conn.commit()

conn = connect(app.config)
geogroups = readGeoGroupings()
# print geogroups
geogroupings = 'groups',len(geogroups['geogroupings'])
buildings  = 'buildings', len( geogroups['buildings'])
print geogroupings
print buildings
counter = 0
for building in geogroups['buildings']:
    #print '%s %f %f %s' %(building['name'],float(building['latitude']),float(building['longitude']),building['id'])
    # print building
    name = building['name'].encode('unicode_escape')
    building['name']=name
    #print '%d %s' %(counter,name)
    counter +=1
    #print '[%s] %f %f' %(name,float(building['latitude']),float(building['longitude']))
    retval = checkWeworkId(building['id'])
    # print retval[0]
    if retval == None or retval[0][0]==0:
        print 'Not found in wework_tranfer [%s]' % building['name']
        insertWeworkEntry(building)
    #    else:
        #    print '[%s] exists in wework_transfer' % building['name']
    #check if id matches wework_id, if not insert
    # if id found update

searchList = searchWeworkTransfer()
counter = 0
print 'wework transfer entries length',len(searchList)
for each in searchList:
    name = each[0]
    results = searchLocation(name)
    print 'weworktransfer id %d: %s length %d'%(each[1],each[0],len(results))
    for i in results:
        print 'location id:%d name:%s'%(i[0],i[1])
        updateWeworkTransfer(each[1],i[0])

if __name__ == '__main__':
    # port = int(os.environ.get('PORT',4000))
    app.run(host='0.0.0.0', use_reloader=False, port=7171, debug=True)
