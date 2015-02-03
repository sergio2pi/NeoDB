from core import Project
from core import Individual
import psycopg2
import ConfigParser
import base64
import os
import dbutils

def get_id(connection, table_name, **kwargs):
    """
    Use:
    connection = neodb.dbconnect(name, username, password, host)
    
    # Returns id of project with name "projectname" 
    [(id, _)] = get_id(connection, "project", name = "projectname")
    
    # Returns all segments'id between '2014-03-01' and '2014-03-21':
    ids = get_id(connection, "segment", date_start = '2014-03-01', date_end = '2014-03-21')
    
    You can add all parameters as columns the table have.
    Function returns the follow format:
        [(id1, name1), (id2, name2), ...]
    """
    cursor = connection.cursor()
    columns = column_names(table_name, connection)
    
    ids = []
    
    if kwargs == {}:
        query = "SELECT id, name FROM " + table_name
        cursor.execute(query)
        results = cursor.fetchall()
        
        for i in results:
            ids.append((i[0],str(i[1])))
            
    else:
        query = "SELECT id, name FROM " + table_name + " WHERE "
        constraint = ""
        time_constraint = ""
        
        if kwargs.has_key("date_start") and kwargs.has_key("date_end"):
            time_constraint = "date >= '%s' and date <= '%s'"%(kwargs.pop('date_start'),kwargs.pop('date_end'))
        elif kwargs.has_key("date_start"):
            time_constraint = "date >= '%s'"%kwargs.pop('date_start')
        elif kwargs.has_key("date_end"):
            time_constraint = "date <= '%s'"%kwargs.pop('date_end')
            
        for key, value in kwargs.iteritems():
            if key in columns:
                constraint = "%s %s='%s' and "%(constraint,key,value)
            else:
                raise ValueError('%s is not member of %s'%(key,object))
        
        if constraint != "" and time_constraint != "":
            query = query + constraint + time_constraint
        elif time_constraint != "":
            query = query + time_constraint
        elif constraint != "":
            constraint = constraint[0:len(constraint)-5]
            query = query + constraint
    
        cursor.execute(query)
        results = cursor.fetchall()
        
        for i in results:
            ids.append((i[0],str(i[1])))
        
    return ids
    
def column_names(table_name, connection):
    cursor = connection.cursor()        
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'"%table_name)
    results = cursor.fetchall()
    
    columns = []
    for i in results:
        columns.append(str(i[0]))
    
    return columns

if __name__ == '__main__':
    
    pass
