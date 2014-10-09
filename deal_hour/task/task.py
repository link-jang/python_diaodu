'''
Created on Apr 28, 2014

@author: conny
'''
from util.mysql_util import mysql_pool
from util import log_util



class Task(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        
        self.mysql_pool=None
        self.conn=None
        self.cursor=None
        self.get_db()
    
        
        
    def get_db(self):
        if self.conn==None:
            self.mysql_pool=mysql_pool()
            self.conn=self.mysql_pool.conn_object
            self.cursor=self.conn.cursor()
        if self.cursor==None:
            self.cursor=self.conn.cursor()
    
    def close_db(self):
        
        if self.cursor!=None:
            self.cursor.close()
            self.cursor=None
            
        if self.conn!=None:
            self.mysql_pool.putback()
            self.conn=None
    
    def deal_hour_task(self,day,hour):
        raise 'do not exec parent method'
    
    
    def deal_day_task(self,day):
        raise 'do not exec parent method'
    
    


        
if __name__ == '__main__':
    task=Task()
    task.get_db_res_list('tel','blfact', '20140424')
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            