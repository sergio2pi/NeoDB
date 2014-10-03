'''
Created on Sep 8, 2014

@author: sergio
'''
import neo.core
import neodb
import psycopg2

class RecordingChannelDB(neo.core.RecordingChannel):
    '''
    classdocs
    '''
    def __init__(self, id_block = None, id_recordingchannelgroup = None, index = None,
                        coordinate = None, name = None, description = None,
                        file_origin = None):
        '''
        Constructor
        '''
        self.id = None
        self.id_block = id_block
        self.id_recordingchannelgroup = id_recordingchannelgroup
        self.index = index
        self.coordinate = coordinate
        self.name = name
        self.description = description
        self.file_origin = file_origin
    
    def save(self, connection):
        '''
        Save Recordingchannel into database trough connection. 'connection" is a Psycopg2 connection. You can get
        connection using neodb.connectdb().
        '''
        if self.id_block == None and self.id_recordingchannelgroup == None:
            raise StandardError("RecordingChannel must have a id of Session Block or Recording Channel Group.")
        
        if self.index == None:
            raise StandardError("RecordingChannel must have a index")
        
        cursor = connection.cursor()
        query = """INSERT INTO recordingchannel (id_block, id_recordingchannelgroup,
                                                 index, coordinate, name, 
                                                 description, file_origin)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(query,[self.id_block, self.id_recordingchannelgroup, self.index, self.coordinate, self.name, self.description, self.file_origin])
        connection.commit()
        
        if self.id_block and self.id_recordingchannelgroup:
            [(id, _)] = neodb.get_id(connection, 'recordingchannel',
                                     id_block = self.id_block, 
                                     id_recordingchannelgroup = self.id_recordingchannelgroup, 
                                     index = self.index)
        elif self.id_recordingchannelgroup:
            [(id, _)] = neodb.get_id(connection, 'recordingchannel', 
                                     id_recordingchannelgroup = self.id_recordingchannelgroup, 
                                     index = self.index)
        elif self.id_block:
            [(id, _)] = neodb.get_id(connection, 'recordingchannel', 
                                     id_block = self.id_block, 
                                     index = self.index)
        return id
        
if __name__ == '__main__':
    username = 'postgres'
    password = 'postgres'
    host = '192.168.2.2'
    dbname = 'demo'
    url = 'postgresql://%s:%s@%s/%s'%(username, password, host, dbname)
    
    dbconn = psycopg2.connect('dbname=%s user=%s password=%s host=%s'%(dbname, username, password, host))
    
    rc = RecordingChannelDB(id_block=50, index = 0)
    id = rc.save(dbconn)
    print id
    rc = RecordingChannelDB(index = 0)
    id = rc.save(dbconn)