'''
Created on Sep 4, 2014

@author: sergio
'''
import neo.core
import numpy
import psycopg2
import neodb
from quantities import s

class SpikeDB(neo.core.Spike):
    '''
    classdocs
    '''
    def __init__(self, id_unit = None, id_segment = None, id_recordingchannel = None,
                        time = None, waveform = None, left_sweep = None, 
                        sampling_rate = None, name = None, description = None,
                        file_origin = None, index = None):
        self.id = None
        self.id_unit = id_unit
        self.id_segment = id_segment
        self.id_recordingchannel = id_recordingchannel
        self.index = index

        if time != None:
            if type(time) == numpy.float64:
                self.time = numpy.array(time)*s
            else:
                self.time = float(time.simplified)
        else:
            self.time = numpy.array(0.0)*s
        
        self.waveform = waveform
        
        if left_sweep != None:
            self.left_sweep = float(left_sweep.simplified)
        else:
            self.left_sweep = left_sweep
        
        if sampling_rate != None:
            self.sampling_rate = float(sampling_rate.simplified)
        else:
            self.sampling_rate = sampling_rate
        
        self.name = name
        self.description = description
        self.file_origin = file_origin

    def save(self, connection):
        # Check mandatory values
        if self.id_segment == None:
            raise StandardError("Spike must have id_segment.")
        
        if self.waveform == []:
            raise StandardError("Spike must have a signal (waveform).")
        
        if self.index == []:
            raise StandardError("""Spike must have a index, it is the index of
                                   the maximum point of the signal.""")
        
        if self.left_sweep != None:
            left_sweep = float(self.left_sweep)
        else:
            left_sweep = self.left_sweep
        
        if self.time != None:
            time = float(self.time)
        else:
            time = self.time
        
        if self.sampling_rate != None:
            sampling_rate = float(self.sampling_rate)
        else:
            sampling_rate = self.sampling_rate
        
        # Format signal
        signalb = numpy.int16(self.waveform)
        
        # QUERY
        cursor = connection.cursor()
        
        if self.id == None:
            query = """INSERT INTO spike 
                   (id_unit, id_segment, time, waveform, left_sweep,
                    sampling_rate, name, description, file_origin, index)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(query,[self.id_unit, self.id_segment,
                                  self.id_recordingchannel, float(time),
                                  psycopg2.Binary(signalb), left_sweep,
                                  sampling_rate,  self.name,
                                  self.description, self.file_origin,
                                  self.index])
        else:
            query = """ UPDATE public.spike SET id_unit = %s, id_segment = %s,
                        id_recordingchannel = %s,
                        TIME = %s, waveform = %s, left_sweep = %s,
                        sampling_rate = %s, name = %s, description = %s,
                        file_origin = %s, index = %s
                        WHERE id = %s"""
            cursor.execute(query,[self.id_unit, self.id_segment, 
                                  self.id_recordingchannel, float(self.time),
                                  psycopg2.Binary(signalb), float(self.left_sweep),
                                  int(self.sampling_rate),  self.name,
                                  self.description, self.file_origin,
                                  self.index])
        
        connection.commit()
        
        # Get ID
        [(id, _)] = neodb.get_id(connection, 'spike', 
                                 index = self.index, 
                                 id_segment = self.id_segment)
        
        self.id = id
        return id

def get_from_db(connection, id_block, channel):
    
    if id_block == None:
        raise StandardError(""" You must specify id_block.""")
    
    if channel == None:
        raise StandardError(""" You must specify number of channel.""")
    
    # QUERY
    cursor = connection.cursor()
    
    query = """select analogsignal.index,
               analogsignal.signal,
               analogsignal.name
               from analogsignal join segment
               on id_segment = segment.id
               and analogsignal.channel_index = %s"""%nchannel 

if __name__ == '__main__':
    username = 'postgres'
    password = 'postgres'
    host = '192.168.2.2'
    dbname = 'demo'
    url = 'postgresql://%s:%s@%s/%s'%(username, password, host, dbname)
    
    dbconn = psycopg2.connect('dbname=%s user=%s password=%s host=%s'%(dbname, username, password, host))

    spike = SpikeDB(id_segment = 33, waveform = [1,2,3,4,5,6], index = 156)
    spike.save(dbconn)
    pass
        