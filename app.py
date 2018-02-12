import urllib3
import json
import itertools

import certifi

# urllib3.contrib.pyopenssl.inject_into_urllib3()

http = urllib3.PoolManager( cert_reqs='CERT_REQUIRED',   ca_certs=certifi.where())

def readGeoGroupings():
    #response = urllib2.urlopen('https://api-proxy.wework.com/locations/api/v1/geogroupings')
    #a = response.read()
    #return json.loads(a)
    string = 'https://api-proxy.wework.com/locations/api/v1/geogroupings'
    return urlread(string)

def readGeoGroup(group):
    response = urllib3.urlopen('https://api-proxy.wework.com/locations/api/v1/geogroupings/'+group)
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

geogroups = readGeoGroupings()
#print a
b = geogroups['geogroupings']
#print 'there are %d groups' %(len(b))
building_count = 0
building_list = []
building_list_info = []
for group in b:
    #print group.keys()
    # name, address, phone
    # to get geogroup use slug
    # for key in group.keys():
    #   print key, '##', group[key]
    slug = group['slug']
    subgroup = readGeoGroup(slug)
    region = subgroup['geogrouping']
    #print 'there are %d subgroups' % (len(region))
#
#     #print region.keys()
    buildings = len(region['buildings'])
    building_count += buildings
    print '%s has [%d]buildings:' %(region['name'],buildings)
    for building in region['buildings']:
        building_list.append(building['slug'])
building_set = set(building_list)

building_count = len(building_set)
        #print 'there are %d buildings' % building_count
building_list2 = sorted(list(building_set))
        #

        # for building in building_list2:
        #     print 'name:',building
        #     building_info = readBuilding(building)
        #     print 'keys',building_info.keys()
        #     print building_info['building']
        #     buliding_list_info.append(building_info)
print processGeoGroupings()
print 'processing subgroups'
print processSubGeogroup()
print generate_building_list()