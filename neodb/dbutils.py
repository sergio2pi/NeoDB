'''
Created on Apr 20, 2014

@author: sergio
'''
import re
import datetime
import psycopg2
import paramiko
import quantities

class SSHConnection(object):
    """"""
 
    #----------------------------------------------------------------------
    def __init__(self, host, username, password, port=22):
        """Initialize and setup connection"""
        self.sftp = None
        self.sftp_open = False
 
        # open SSH Transport stream
        self.transport = paramiko.Transport((host, port))
 
        self.transport.connect(username=username, password=password)
 
    #----------------------------------------------------------------------
    def _openSFTPConnection(self):
        """
        Opens an SFTP connection if not already open
        """
        if not self.sftp_open:
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            self.sftp_open = True
 
    #----------------------------------------------------------------------
    def get(self, remote_path, local_path=None):
        """
        Copies a file from the remote host to the local host.
        """
        self._openSFTPConnection()        
        self.sftp.get(remote_path, local_path)        
 
    #----------------------------------------------------------------------
    def put(self, local_path, remote_path=None):
        """
        Copies a file from the local host to the remote host
        """
        self._openSFTPConnection()
        self.sftp.put(local_path, remote_path)
 
    #----------------------------------------------------------------------
    def close(self):
        """
        Close SFTP connection and ssh connection
        """
        if self.sftp_open:
            self.sftp.close()
            self.sftp_open = False
        self.transport.close()

def get_ppgdate(date):
    """
    'date' may be a datetime.date type or string with format 'dd-mm-yyyy' or 
    'dd/mm/yyyy'. Function returns psycopg2.Date
    """
    if type(date) == datetime.date:
        return psycopg2.Date(date.year, date.month, date.day)
    
    if type(date) != str:
        raise StandardError("Invalid date type. It must be 'datetime.date' or " + 
                             "string with format 'dd-mm-yyyy' or 'dd/mm/yyyy'")
    
    match = re.match('(^(\d{1,2})[\/|-](\d{1,2})[\/|-](\d{4})$)', date)
    if match:
        dd = int(match.groups()[1])
        mm = int(match.groups()[2])
        yyyy = int(match.groups()[3])
        if not((1<dd<31) or (1<mm<12)):
            raise StandardError("Invalid month or day value. Format: 'dd-mm-yyyy' or 'dd/mm/yyyy'")
        
        return psycopg2.Date(yyyy,mm,dd)
    else:
        raise StandardError("Invalid date format. It must be 'dd-mm-yyyy' or 'dd/mm/yyyy'")


def get_datetimedate(date):
    """
    'date' must be a string with format 'dd-mm-yyyy' or 
    'dd/mm/yyyy'. Function returns datetime.date
    """
    if type(date) != str:
        raise StandardError("Invalid date type. It must be 'datetime.date' or " + 
                             "string with format 'dd-mm-yyyy' or 'dd/mm/yyyy'")
    
    match = re.match('(^(\d{1,2})[\/|-](\d{1,2})[\/|-](\d{4})$)', date)
    if match:
        dd = int(match.groups()[1])
        mm = int(match.groups()[2])
        yyyy = int(match.groups()[3])
        if not((1<=dd<=31) or (1<=mm<=12)):
            raise StandardError("Invalid month or day value. Format: 'dd-mm-yyyy' or 'dd/mm/yyyy'")
        
        return datetime.date(yyyy,mm,dd)
    else:
        raise StandardError("Invalid date format. It must be 'dd-mm-yyyy' or 'dd/mm/yyyy'")

def get_quantitie(unit):
    if unit not in ['V', 'v', 'mV', 'mv', 'uV', 'uv']:
        raise StandardError("Parameter must be 'V' , 'mV' or 'uV'")
    
    if unit == 'V' or unit == 'v':
        return quantities.V
    if unit == 'mV' or unit == 'mv':
        return quantities.mV
    if unit == 'uV' or unit == 'uv':
        return quantities.uV

if __name__ == '__main__':
    print get_ppgdate(3)