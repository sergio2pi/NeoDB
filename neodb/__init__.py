from core import Project
from core import Individual
import psycopg2
import ConfigParser
import base64
import os
import dbutils

def get_config_folder():
    base_path = os.path.expanduser('~')
    path = base_path + '/.neodb'
    if os.path.isdir(path) == False:
        os.mkdir(path)
    
    return path

def save_config(key, value):
    if key in ['host', 'user', 'password']:
        section = 'server'
    elif key in ['dbname', 'dbuser', 'dbpassword']:
        section = 'database'
    else:
        raise StandardError("Invalid key '%s'."%key)
    
    fichero = get_config_folder() + "/config.ini"
    parser = ConfigParser.SafeConfigParser()
    try:
        sf = open(fichero,"r+")
    except IOError:
        sf = open(fichero,"w")
        
        setsection = 'server'
        parser.add_section(setsection)
        parser.set(setsection, 'host', '')
        parser.set(setsection, 'user', '')
        parser.set(setsection, 'password', '')
        
        setsection = 'database'
        parser.add_section(setsection)
        parser.set(setsection, 'dbname', '')
        parser.set(setsection, 'dbuser', '')
        parser.set(setsection, 'dbpassword', '')
        
    parser.read(fichero)
    parser.set(section, key, value)
    
    parser.write(sf)
    sf.close()

def read_config(key):
    if key in ['host', 'user', 'password']:
        section = 'server'
    elif key in ['dbname', 'dbuser', 'dbpassword']:
        section = 'database'
    else:
        raise StandardError("Invalid key '%s'."%key)
    
    fichero = get_config_folder() + "/config.ini"
    parser = ConfigParser.SafeConfigParser()
    
    try:
        sf = open(fichero,"r+")
    except IOError:
        sf = open(fichero,"w")
        setsection = 'server'
        parser.add_section(setsection)
        setsection = 'database'
        parser.add_section(setsection)
        sf.close()
        
    parser.read(fichero)
    value = parser.get(section,key)
    
    if value == '':
        raise StandardError("Key '%s' is not configured."%key)
    
    return value
    
def config_server(host, username, password, dbname, dbuser, dbpassword):
    save_config('host', host)
    save_config('user', username)
    save_config('password', password)
    save_config('dbname', dbname)
    save_config('dbuser', dbuser)
    save_config('dbpassword', dbpassword)

def dbconnect(dbname = None, username = None, password = None, host = None):
    if not dbname:
        dbname = read_config('dbname')
    if not username:
        username = read_config('dbuser')
    if not password:
        password = read_config('dbpassword')
    if not host:
        host = read_config('host')
    
    dbconn = psycopg2.connect('dbname=%s user=%s password=%s host=%s'%(dbname, username, password, host))
    return dbconn

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
