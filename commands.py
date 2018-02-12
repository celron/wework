import app as app
import json
import argparse

def parse_range(astr):
    result = set()
    for part in astr.split(','):
        x = part.split('-')
        result.update(range(int(x[0]), int(x[-1]) + 1))
    return sorted(result)

parser = argparse.ArgumentParser()
parser.add_argument('-n', type=parse_range)
args = parser.parse_args()
print(args.n)


def command_loop():
    command = raw_input('enter command>>')
    return command


def getInt(string):
    if string.isdigit():
        return int(string)
    return None


def building_command(string):
    print 'building command'
    parameters = string.split(' ')
    if parameters[0] == 'load':
        print 'loading building data'
        app.processBuildings()
    if parameters[0] == 'len':
        print len(app.building_data)
    if parameters[0] == 'list':
        print 'list parameters'
        a = getInt(parameters[1])
        if a is not None and a <= len(app.building_data):
            print app.building_data[a]
    if parameters[0] == 'keys':
        print 'printing keys'
        printKeys(app.building_data['building'])
    if parameters[0] == 'key' and len(parameters)>1:
        a = getInt(parameters[1])
        if a is not None:
            print app.building_data['building'][a]
        else:
            print app.building_data['building'][parameters[1]]
    return None

#
# def region_command(string):
#     print 'region'
#     parameters = string.split(' ')
#     array = app.subgeogroup_data
#     # keys [index]
#     if parameters[0] == 'keys':
#         handleKeys(array,parameters)
#
#     # design is region key [key] [index default=0]
#     if parameters[0] == 'data':
#         handleData(array,parameters)
#
#     # format of command is 'key' 'keyname' [index default 0] (use a range? 1..10?)
#     if parameters[0] == 'key' and len(parameters) > 1:
#         handleKey(array,parameters)
#     return None


class ListNavigator:
    array = ''
    feed = ''
    def __init__(self):
        self.array = ''

    def command(self, string):
        parameters = string.split(' ')
        #array = app.subgeogroup_data
        # keys [index]
        if parameters[0] == 'keys':
            return self.handleKeys(self.array, parameters)

        # design is region key [key] [index default=0]
        if parameters[0] == 'data':
            return self.handleData(self.array, parameters)

        # format of command is 'key' 'keyname' [index default 0] (use a range? 1..10?)
        if parameters[0] == 'key' and len(parameters) > 1:
            return self.handleKey(self.array, parameters)
        return None

    def handleKeys(self, array, parameters):
        print 'printing keys'
        no_parameters = len(parameters)
        index = 0
        if no_parameters > 1:
            index = getInt(parameters[1])  # parse_range(parameters[1])  #getInt(parameters[1])
        self.printKeys(array, index)


    def handleData(self, array,parameters):
        no_parameters = len(parameters)
        index = ['0']
        key = None
        if no_parameters > 1:
            index = parse_range(parameters[1])
            key = None
        if no_parameters > 2:
            key = parameters[2]
        return self.printData(array, index, key)


    def handleKey(self, array, parameters):
        index = 0
        no_parameters = len(parameters)
        if no_parameters > 0:
            a = getInt(parameters[0])
        if no_parameters > 2:
            index = getInt(parameters[2])
            if index > len(array):
                return {'error': 'index %d is greater than array size of %d' % (index, len(array))}
                #return None
        if index is None:
            index = 0
        try:
            if a is not None:
                # print "[%s]:" % (index, array[index][a])
                return {'index': index, 'key': a, 'data': array[index][a]}
            else:
                #print "index %d:[%s]: %s" % (index, parameters[1], array[index][parameters[1]])
                return {'index': index, 'key': parameters[1], 'data': array[index][parameters[1]]}
        except KeyError, e:
            #print 'key [%s] does not exist: %s' % (parameters[1], e)
            return {'error':'key [%s] does not exist: %s' % (parameters[1], e)}


    def printKeys(self, array, index_list):
        if index_list not in locals():
            index_list = [0]
        array_size = len(array)

        for index in index_list:
            data = array[index]
            for item in data.keys():
                print item


    def printData(self, array, index_range, key_list):
        return_array = []
        # if type(index_range) is not dict:
        #    index_range = [0]
        for indexd in index_range:
            index = int(indexd)
            if index+1 > len(array) or array =='':
                continue
                # return {'error': 'index out of range, array size %d' % (len(array))}
            try:
                data = array[index]
            except IndexError:
                continue
            # if key is None print all
            output = dict()
            if key_list is None:
                for item in data.keys():
                    output[item] = data[item] # print '[%s]: %s' % (item, data[item])
                return_array.append(output)
            else:
                parse_list = key_list.split(",")
                line_data = dict()
                for key in parse_list:
                    try:
                        line_data[key]= data[key]
                        # print '[%s]: %s' % (key, data[key])
                    except KeyError, e:
                        a = 1
                        #return {'error': 'key [%s] does not exist' % key}
                return_array.append(line_data)
        return return_array

def subregion_command(string):
    print 'subregion'
    return None


def load_command(string):

    if string == 'basic':
        print 'loading data, please wait...'
        feeds.loadBasicData()
        region_handler.array = feeds.subgeogroup_data
        geo_handler.array = feeds.geogroups_data
        building_handler.array = feeds.building_data
    if string == 'building':
        print 'load building data, please wait...'
        feeds.processBuildings()
        building_handler.array = feeds.building_data
    return None



def info():
    return {'regions:': len(feeds.geogroups_data),
        'sub geogroup': len(feeds.subgeogroup_data),
        'building index': len(feeds.building_list),
        'building list': len(feeds.building_data)}



def process_command(command, string):
    if command == 'info' or command == 'status':
        return info()
    if command == 'load':
        return load_command(string)
    if command == 'building':
        return json.dumps(building_handler.command(string))
    if command == 'geo':
        return geo_handler.command(string)
    if command == 'region':
        return json.dumps(region_handler.command(string))

    return 'Unknown command:',command

var = 1
count = 0

feeds = app.WeworkFeeds()
region_handler = ListNavigator()
building_handler = ListNavigator()
geo_handler = ListNavigator()

while var == 1:
    command_string = command_loop()
    commands = command_string.partition(' ')
    if commands[0] == 'exit':
        var = 0
    #print 'command [%s] parameters:%s' % (commands[0], commands[2])
    print process_command(commands[0],commands[2])



