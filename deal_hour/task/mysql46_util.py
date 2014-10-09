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

class mysql46_conn(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.mysql_pool=None
        self.config=conf_util.config_singleton().confif_dict
        self.connect_db()
        
        
    def connect_db(self):  
        
        mysql_pool_count=self.config['mysql_pool_count46']
        mysql_ip=self.config['host46']
        mysql_port=self.config['port46']
        mysql_db=self.config['db46']
        mysql_user=self.config['user46']
        mysql_pwd=self.config['passwd46']
        self.mysql_pool=Queue()
        
        for count in range(0,mysql_pool_count,1):
            conn=MySQLdb.connect(host=mysql_ip,user=mysql_user,passwd=mysql_pwd,db=mysql_db,charset="utf8",port=int(mysql_port))
            self.mysql_pool.put(conn)
            print 'mysql 46 connet success,session: '+str(count)        
        
class mysql46_pool(object):
    def __init__(self):
        mutex=get_mutex()
         
    def getconn(self):
        mutex = get_mutex()
        mysql_pool_obj=mysql_singleton()
        
        if not mysql_pool_obj.mysql_pool.empty():
            mutex.acquire()
            conn_object = mysql_pool_obj.mysql_pool.get()
            mutex.release()
            print 'get mysql46 pool queue size :'+str(mysql_pool_obj.mysql_pool.qsize())
        
        if conn_object != None:
            return conn_object
        else:
            return None

    def putback(self,conn_object):
        mutex=get_mutex()
        #print conn_object
        if conn_object != None:
            
            mutex.acquire()
            mysql_pool_obj=mysql_singleton()
            mysql_pool_obj.mysql_pool.put(conn_object)
            #conn_object.close() 
            conn_object = None 
            print 'put back mysql46 pool queue size :'+str(mysql_pool_obj.mysql_pool.qsize())
            mutex.release()
            
def get_mutex():
    global mutex
    if mutex==None:
        mutex=threading.Lock()
    return mutex

def mysql_singleton():
    global _mysql_singleton
    if  not _mysql_singleton:
        _mysql_singleton = mysql46_conn()
        print 'init _resource_singleton 46'
        return _mysql_singleton
    else:
        print '_resource_singleton already inited 46'   
        return _mysql_singleton

        
        
if __name__=='__main__':
    
    pool = mysql46_pool()
    pool.getconn()
    #a = pool.conn_object
    #pool.putback()
    pool2 = mysql46_pool()
    pool2.getconn()
    pool3 = mysql46_pool()
    pool4 = mysql46_pool()
    #a = mysql_singleton()
    #b = mysql_singleton() 
    
    
    
