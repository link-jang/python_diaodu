# -*- coding: utf-8 -*-
import sys
import os
import threading
import time
mysql = '/usr/local/mysql/bin/mysql -uroot -psd-9898w -N -e '
mysql75 = '/usr/local/mysql/bin/mysql -h10.1.1.175 -uxldc -psd-9898w -N -e '
table = 'zyzx_data_static_db.ftbl_duanyou_schedule'
mutex = threading.Lock()
def genersql(mysql,sql):
    return mysql + ' "' + sql + '"'

def concatsql(day,game,version,key,value):
    return mysql+' "replace into zyzx_data_static_db.ftbl_duanyou_day(fday,fversion,fgame,fkey,fvalue)value(\''+str(day)+'\',\''+str(version)+'\',\''+str(game)+'\',\''+str(key)+'\',\''+str(value)+'\');"'

def execmd(cmd):
    pipe=os.popen('{ ' +cmd+'; } 2>> ../log/python.log')
        #pipe=os.popen('{ ' + cmd+';}')
    text=pipe.read()
    sts=pipe.close()

    if sts==None:
        sts=0
    if text[-1:] == '\n': text = text[:-1]
    return sts, text
def log(text):
    print  '%s: %s' %(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),text)

if __name__ == '__main__':
    log('text')
