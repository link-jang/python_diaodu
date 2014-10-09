'''
Created on Apr 25, 2014

@author: conny
'''
import ConfigParser

global _config_singleton
_config_singleton=None

config_file='../conf/conf.ini'
class Config(object):
    conf = ConfigParser.SafeConfigParser()
    def __init__(self):
        self.confif_dict={}
        self.conf.read(config_file)
        self.readOption()
    
    def readOption(self):
        self.confif_dict['host75']=self.conf.get('mysql75','host')
        self.confif_dict['port75']=int(self.conf.get('mysql75','port'))
        self.confif_dict['user75']=self.conf.get('mysql75','user')
        self.confif_dict['passwd75']=self.conf.get('mysql75','passwd')
        self.confif_dict['db75']=self.conf.get('mysql75','db')
        self.confif_dict['mysql_pool_count75']=int(self.conf.get('mysql75','mysql_pool_count'))
        
        self.confif_dict['host46']=self.conf.get('mysql46','host')
        self.confif_dict['port46']=int(self.conf.get('mysql46','port'))
        self.confif_dict['user46']=self.conf.get('mysql46','user')
        self.confif_dict['passwd46']=self.conf.get('mysql46','passwd')
        self.confif_dict['db46']=self.conf.get('mysql46','db')
        self.confif_dict['mysql_pool_count46']=int(self.conf.get('mysql46','mysql_pool_count'))
        self.confif_dict['insert_data_url']=self.conf.get('hive','insert_data')
        #self.confif_dict['mysql_pool_count']=int(self.conf.get('mysql','mysql_pool_count'))
        #self.confif_dict['email_server']=self.conf.get('email','server')
        #self.confif_dict['email_user']=self.conf.get('email','user')
        #self.confif_dict['email_passwd']=self.conf.get('email','passwd')
   
        
        
def config_singleton():
    global _config_singleton
    if  not _config_singleton:
        _config_singleton = Config()
        return _config_singleton
    else:
        return _config_singleton
        
if __name__=='__main__':
    config=config_singleton()
    print config.confif_dict['mysql_pool_count']
    
    
