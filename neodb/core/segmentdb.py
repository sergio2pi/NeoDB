'''
Created on Apr 20, 2014

@author: sergio
'''
import psycopg2
import neodb
import neo.core
import neodb.dbutils

class SegmentDB(neo.core.Segment):
    '''
    classdocs
    '''
    def __init__(self, id_block = None, name = None, description = None,
                       file_origin = None, file_datetime = None,
                       rec_datetime = None, index = None):
        '''
        Constructor
        '''
        neo.core.Segment.__init__(self, name, description, file_origin, file_datetime,
                                  rec_datetime, index)
        
        self.id_block = id_block
        
    def save(self, connection):
        # Check mandatory values
        if self.id_block == None:
            raise StandardError("Segment must have id_block.")
        
        if self.file_origin == None:
            raise StandardError("Segment must have a file_origin to process.")
        
        other = neodb.get_id(connection, 'segment', name = self.file_origin)
        if other != []:
            raise StandardError("There is another segment with name '%s'."%self.name)
        
        file_datetime = None
        rec_datetime = None
        if self.file_datetime:
            file_datetime = neodb.dbutils.get_ppgdate(self.file_datetime)
        if self.rec_datetime:
            rec_datetime = neodb.dbutils.get_ppgdate(self.rec_datetime)
        
        # QUERY
        cursor = connection.cursor()
        
        query = """INSERT INTO segment 
                   (id_block, name, description, file_origin, file_datetime,
                   rec_datetime, index)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        
        cursor.execute(query,[self.id_block, self.name, self.description,
                              self.file_origin, file_datetime, rec_datetime,
                              self.index])
        
        connection.commit()
        
        # Get ID
        id = None
        getid = neodb.get_id(connection, 'segment', name = self.name, file_origin = self.file_origin)
        if getid:
            [(id, _)] = getid
        
        return id
        