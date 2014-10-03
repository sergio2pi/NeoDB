'''
Created on Apr 18, 2014

@author: sergio
'''
import psycopg2
import neodb
import neodb.dbutils as dbutils

class Project(object):
    '''
    Project gather all information and registers about a project of experimental in a individual.
    '''
    def __init__(self, name = None, date = None, description = None, index = None):
        '''
        name : name of project (25 characters max.)
        date : date of start project. Format dd-mm-yyyy or dd/mm/yyy.
        description: aditional information (150 characters max.)
        index: number of internal reference.
        
        use:
        project = neodb.core.Project("project1", "19-05-2014", "Cognital analisys")
        '''
        self.id = None
        self.index = index
        self.name = name
        self.description = description
        
        if date:
            self.date = dbutils.get_ppgdate(date)
        else:
            self.date = date 

    def save(self, connection):
        '''
        Save Project into database trough connection. 'connection" is a Psycopg2 connection. You can get
        connection using neodb.connectdb().
        '''
        if self.name == None:
            raise StandardError("Project must have a name.")
        
        other = neodb.get_id(connection, 'project', name = self.name)
        
        if other != []:
            raise StandardError("There is another project with name '%s'."%self.name)
        
        cursor = connection.cursor()
        query = """INSERT INTO project (name, date, description, index)
                   VALUES (%s, %s, %s, %s)"""
        cursor.execute(query,[self.name, self.date, self.description, self.index])
        connection.commit()
        
        [(id, _)] = neodb.get_id(connection, 'project', name = self.name)
        
        return id
    
    def get_from_db(self, connection, id):
        cursor = connection.cursor()
        query = """ SELECT * FROM project WHERE id = %s"""
        cursor.execute(query, [id])
        results = cursor.fetchall()
        
        if results != []:
            self.name =          results[0][2]
            self.description =   results[0][3]
            self.date =   results[0][4]
        
        results = {}
        results['name'] = self.name
        results['description'] = self.description
        results['date'] = self.date
        
        return results
        
if __name__ == '__main__':
    username = 'postgres'
    password = 'postgres'
    host = '192.168.2.2'
    dbname = 'demo'
    url = 'postgresql://%s:%s@%s/%s'%(username, password, host, dbname)
    
    dbconn = psycopg2.connect('dbname=%s user=%s password=%s host=%s'%(dbname, username, password, host))
    project = Project("nameprueba2","19-05-2014","description")
    project.save(dbconn)
    