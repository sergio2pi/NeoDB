'''
Created on Apr 20, 2014

@author: sergio
'''

import psycopg2
import neodb
import os
import neodb.dbutils as dbutils

class Individual(object):
    '''
    Individual contains iformation about a research individual. Like name, days old, etc. 
    '''

    def __init__(self, name = None, description = None, birth_date = None, picture_path= None, index = None):
        '''
        Constructor
        '''
        self.name = name
        self.description = description
        self.birth_date = birth_date
        self.picture_path = picture_path
        self.index = index
        
        if birth_date:
            self.birth_date = dbutils.get_ppgdate(birth_date)
        else:
            self.birth_date = birth_date
        
    def save(self, connection):
        if self.name == None:
            raise StandardError("Individual must have a name.")
        
        other = neodb.get_id(connection, 'individual', name = self.name)
        
        if other != []:
            raise StandardError("There is another individual with name '%s'."%self.name)
        
        cursor = connection.cursor()
        
        if self.picture_path is not None:
            if os.path.isfile(self.picture_path):
                picture_dst = self.__copy_picture(self.picture_path)
            else:
                raise StandardError("'%s' is not a valid path, it does not exist or application can not access."%self.picture_path)
        
        query = """INSERT INTO individual (name, description, birth_date, 
                   picture, index) VALUES (%s, %s, %s, lo_import(%s), %s)"""
        cursor.execute(query,[self.name, self.description, self.birth_date, picture_dst, self.index])
        connection.commit()
        
        [(id, _)] = neodb.get_id(connection, 'individual', name = self.name)
        return id
    
    def __copy_picture(self, picture_path, dest = 'host'):
        
        host = neodb.read_config('host')
        username = neodb.read_config('user')
        pw = neodb.read_config('password')
 
        origin = picture_path
        picturename = origin.split('/')[-1]
        dst = '/tmp/' + picturename
 
        ssh = dbutils.SSHConnection(host, username, pw)
        if dest == 'host':
            ssh.put(origin, dst)
        else:
            ssh.get(origin, dst)
        ssh.close()
        
        return dst
    
    def get_from_db(self, connection, id):
        cursor = connection.cursor()
        query = """ SELECT * FROM individual WHERE id = %s"""
        cursor.execute(query, [id])
        results = cursor.fetchall()
        
        if results != []:
            self.name =          results[0][2]
            self.description =   results[0][3]
            self.birth_date =   results[0][5]
            
            if results[0][5] != None:
                picture_path = '/tmp/%s.jpg'%self.name
                query = """SELECT lo_export( %s, %s )"""
                cursor.execute(query, [results[0][4], picture_path])
                self.picture_path = '/tmp/%s.jpg'%self.name
                self.__copy_picture(self.picture_path, 'local')
        
        results = {}
        results['name'] = self.name
        results['description'] = self.description
        results['birth_date'] = self.birth_date
        results['picture_path'] = self.picture_path
        
        return results
        

if __name__ == '__main__':
    username = 'postgres'
    password = 'postgres'
    host = '192.168.2.2'
    dbname = 'demo'
    url = 'postgresql://%s:%s@%s/%s'%(username, password, host, dbname)
    
    dbconn = psycopg2.connect('dbname=%s user=%s password=%s host=%s'%(dbname, username, password, host))
    #individual = Individual("ind2","description", "03-05-2014", "/home/sergio/Pictures/borracho.jpg")
    #individual.save(dbconn)
    individual = Individual()
    individual.get_from_db(dbconn, 10)
    pass