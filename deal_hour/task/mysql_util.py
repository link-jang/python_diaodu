'''
Created on Apr 25, 2014

@author: conny
'''

import MySQLdb
import conf_util
from Queue import Queue
import threading

global mutex
mutex = threading.Lock()

global _mysql_singleton
_mysql_singleton=None

class mysql_conn(object):
    '''
    classdocs
    '''

    def __init__(self,id):
        '''
        Constructor
        '''
        self.id = None
        self.id = id
        self.mysql_pool=None
        self.config=conf_util.config_singleton().confif_dict
        self.connect_db()
        
        
    def connect_db(self):  
        
        mysql_pool_count = ''
        mysql_ip = ''
        mysql_port = 0
        mysql_db = ''
        mysql_user = ''
        mysql_pwd = ''
        mysql_pool_count=self.config['mysql_pool_count' + self.id]
        mysql_ip=self.config['host' + self.id]
        mysql_port=self.config['port' + self.id]
        mysql_db=self.config['db' + self.id]
        print mysql_db + ' ' + str(self.id)
        mysql_user=self.config['user' + self.id]
        mysql_pwd=self.config['passwd' + self.id]
        self.mysql_pool=Queue()
        
        mutex=get_mutex()
        for count in range(0,mysql_pool_count,1):
            mutex.acquire()
            conn=MySQLdb.connect(host=mysql_ip,user=mysql_user,passwd=mysql_pwd,db=mysql_db,charset="utf8",port=int(mysql_port))
            self.mysql_pool.put(conn)
            print 'mysql ' + self.id + ' connet success,session:'+str(count)        
            mutex.release()
class mysql_pool(object):
    def __init__(self,id):
        self.id = None
        self.id = id
    
    def getconn(self):
        conn_obj = None
        mutex=get_mutex()
        mysql_pool_obj=mysql_singleton(self.id)
        if not mysql_pool_obj.mysql_pool.empty():
            mutex.acquire()
            conn_obj = mysql_pool_obj.mysql_pool.get()
            mutex.release()
            print 'Get mysql ' + self.id + ' pool queue size :'+str(mysql_pool_obj.mysql_pool.qsize())
        if  conn_obj !=None:
            return conn_obj
        else:
            return None


    def putback(self,conn_object):
        mutex=get_mutex()
        if conn_object!=None:
            
            mutex.acquire()
            mysql_pool_obj=mysql_singleton(self.id)
            mysql_pool_obj.mysql_pool.put(conn_object)
            #conn_object.close()
            #conn_object=None
            conn_oject=None
            print 'Put mysql ' + self.id + ' pool queue size :'+str(mysql_pool_obj.mysql_pool.qsize())
            mutex.release()
            
def get_mutex():
    global mutex
    if mutex==None:
        mutex=threading.Lock()
    return mutex

def mysql_singleton(id):
    global _mysql_singleton
    if  not _mysql_singleton:

        _mysql_singleton = mysql_conn(id)
        print '_init _resource_singleton' + id
        return _mysql_singleton
    else:
        print '_resource_singleton already inited'
        return _mysql_singleton

        
    
if __name__=='__main__':
    
    pool = mysql_pool('46')
    pool.getconn()
    #pool.putback()
    pool2 = mysql_pool('75')
    pool2.getconn()
    pool3 = mysql_pool('46')
    pool4 = mysql_pool('46')
    #a = mysql_singleton()
    #b = mysql_singleton() 
    
    
    
