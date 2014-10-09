# -*- coding: utf-8 -*-
import sys
import os
import threading  
import time
import util
import mysql46_util
import mysql75_util
import conf_util
from util import log


class check_timer_schedule(threading.Thread):
    def __init__(self ,num,interval):
        threading.Thread.__init__(self)  
        self.thread_num = num  
        self.interval = interval  
        self.thread_stop = False  
        #self.mysql46 = mysql46_util.mysql46_pool()
        #self.mysql75 = mysql75_util.mysql75_pool()
        self.pool46 = mysql46_util.mysql46_pool()
        self.pool75 = mysql75_util.mysql75_pool()

    def run(self):
        while not self.thread_stop:
            log('Thread check Object' + str(self.thread_num) + 'begin ') 
            conn46 = self.pool46.getconn()
            conn75 = self.pool75.getconn()
            cursor46_sl = conn46.cursor()
            cursor46_up = conn46.cursor() 
            cursor75 = conn75.cursor()
            s_sql = 'select fday,fhour from ftbl_duanyou_schedule where  (fstatus=-3 or fstatus>0)'
            s_param = (time.strftime('%Y%m%d'),time.strftime('%H'))
            #print s_sql
            #print s_param
            #print type(s_param)
            #print util.genersql(sql)
            
            data = cursor46_sl.execute(s_sql)
            #s_hour=[]
            log('number of task with fstatus=-3 or fstatus>0')
            
            for row in cursor46_sl.fetchall():
                
                c_sql75 = 'select fday,fhour from dl_bdl_taskinfo where fday=%s and fhour=%s and fstatus=0 and fbdl_id=\'B3\''
                c_param=(row[0],row[1])
                n = cursor75.execute(c_sql75,c_param)
                for row1 in cursor75.fetchall():
                    #s_hour.append(row1)
                    log('task begin to execute day:' + str(row[0]) + 'task hour:' + str(row[1]) )
                    u_sql46='update  ftbl_duanyou_schedule set fstatus=-1 where fday=%s and fhour=%s'
                    ss= cursor46_up.execute(u_sql46,row1)

            cursor46_sl.close()
            cursor46_up.close()
            cursor75.close()
            self.pool46.putback(conn46)
            self.pool75.putback(conn75)
            time.sleep(self.interval)
    def stop(self):
        self.thread_stop = True
        

class extract_data_hour(threading.Thread):
    def __init__(self ,num,interval):
        threading.Thread.__init__(self)
        self.thread_num = num
        self.interval = interval
        self.insert_data_url = conf_util.Config().confif_dict['insert_data_url']
        self.thread_stop = False
        
        self.pool46 = mysql46_util.mysql46_pool()
    
    
    def run(self):
        while not self.thread_stop:
            conn46 = self.pool46.getconn() 
            cursor46_sl = conn46.cursor()
            cursor46_up = conn46.cursor() 
            
            log('Thread check Object' + str(self.thread_num) + 'begin ') 
            do_sql = 'select fday,fhour from ftbl_duanyou_schedule where  fstatus =-1 limit 1;'
            data = cursor46_sl.execute(do_sql)
            log('task begin to runn:' + str(data))
            for row in cursor46_sl.fetchall():
                u_do_sql='update  ftbl_duanyou_schedule set fstatus=-2 where fday=%s and fhour=%s'
                cursor46_up.execute(u_do_sql,row)
                log('set the status of the task to :-2')
                sts = self.insert_data(row[0],row[1])
                log('result of the bash running status :' + str(sts)) 
                u_do_sql='update  ftbl_duanyou_schedule set fstatus=%s where fday=%s and fhour=%s'
                if sts != 0:
                    cursor46_up.execute(u_do_sql,(sts,row[0],row[1]))
                else:
                    cursor46_up.execute(u_do_sql,(0,row[0],row[1]))
            

            cursor46_sl.close()
            cursor46_up.close()

            self.pool46.putback(conn46)

            time.sleep(self.interval)
    
    
    def insert_data(self,fday,fhour):
        cmd = 'sh ' + self.insert_data_url + ' ' + fday + ' ' + fhour
        #print cmd
        sts,text = util.execmd(cmd)
        #sts,text = util.execmd('sh /home/download/jianglinhe/ziyuanzhongxin/duanyou/bin/deal_hour/hive_job/echo.sh')
        print sts,text
        return sts
    
    def stop(self):
        self.thread_stop = True

    



if __name__ == '__main__':
    thread_1 = check_timer_schedule(1,600)
    thread_1.start()
    time.sleep(10)
    thread_2 = check_timer_schedule(2,600)
    thread_2.start()
    time.sleep(10)

    extract_1 = extract_data_hour(1,600)
    extract_2 = extract_data_hour(2,600)
    #extract_3 = extract_data_hour(3,60)
    extract_1.start()
    time.sleep(10)
    extract_2.start()
    time.sleep(10)
    #extract_3.start()
    print 'main thread'
  
