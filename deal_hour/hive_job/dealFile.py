# -*- coding: utf-8 -*-
import sys
import os


mysql='/usr/local/mysql/bin/mysql -uroot -psd-9898w -N -e'

def concatsql(day,fhour,ftype,game,version,key,value):
    return mysql+' "replace into zyzx_data_static_db.ftbl_duanyou_hour(fday,fhour,ftype,fversion,fgame,fkey,fvalue)value' +\
            '(\''+str(day)+'\',\''+str(fhour)+'\',\''+str(ftype)+'\',\''+str(version)+'\',\''+str(game)+'\',\''+str(key)+'\',\''+str(value)+'\');"'

def execmd(cmd):
    pipe=os.popen('{ ' +cmd+'; } 2>> ../log/python.log')
    text=pipe.read()
    sts=pipe.close()

    if sts==None:
        sts=0
    if text[-1:] == '\n': text = text[:-1]
    return sts, text

#print len(sys.argv)
if len(sys.argv)!=4:
    sys.exit(1)

filename=sys.argv[1]
day=sys.argv[2]
hour=sys.argv[3]
file_object=open(filename)

all_items=list([])
try:
    text=file_object.readline()
    while text:
        text=text.replace('\n','')
        item=text.split('\t')
        all_items.append(item)
        text=file_object.readline()
        
        
finally:
    print 'error'
    file_object.close()

num_dy_valid_show=0
num_dy_download=0
num_dy_del_task=0
num_dy_finisth_download=0
num_dy_install=0
num_dy_close=0
num_dy_install_v2=0
for item in all_items:
    if item[0]=='1907':
        #print concatsql(day,hour,'1',item[1],item[2],'dy_all_show',item[3])
        execmd(concatsql(day,hour,'1',item[1],item[2],'dy_all_show',item[3]))
    elif item[0]=='5054':
        #print concatsql(day,hour,'2',item[1],item[2],'dy_all_show',item[3])
        #print concatsql(day,hour,'2',item[1],item[2],'dy_valid_show',item[3])
        execmd(concatsql(day,hour,'2',item[1],item[2],'dy_all_show',item[3]))
        execmd(concatsql(day,hour,'2',item[1],item[2],'dy_valid_show',item[3]))
    elif item[0]=='5033':
        #print concatsql(day,hour,'1',item[1],item[2],'dy_valid_show',item[3])
        execmd(concatsql(day,hour,'1',item[1],item[2],'dy_valid_show',item[3]))
    elif item[0]=='5047':
        #print concatsql(day,hour,'2',item[1],item[2],'dy_download',item[3])
        execmd(concatsql(day,hour,'2',item[1],item[2],'dy_download',item[3]))
    elif item[0]=='5057':
        #print concatsql(day,hour,'2',item[1],item[2],'dy_del_task',item[3])
        execmd(concatsql(day,hour,'2',item[1],item[2],'dy_del_task',item[3]))
    elif item[0]=='5051':
        #print concatsql(day,hour,'2',item[1],item[2],'dy_finisth_download',item[3])
        execmd(concatsql(day,hour,'2',item[1],item[2],'dy_finisth_download',item[3]))
    elif item[0]=='1910':
        #print concatsql(day,hour,'1',item[1],item[2],'dy_close',item[3])
        execmd(concatsql(day,hour,'1',item[1],item[2],'dy_close',item[3]))
    elif item[0]=='5079':
        execmd(concatsql(day,hour,'2',item[1],item[2],'dy_close',item[3]))

    elif item[0]=='5008' or item[0]=='1909':
        if item[3]==0:
            continue

        num_dy_download=int(item[3])
        for item1 in all_items:
            if item[0]=='5008' and item1[0]=='1909' and item[1]==item1[1] and item1[2]==item[2]:
                num_dy_download+=int(item1[3])
                item1[3]=0
            if item[0]=='1909' and item1[0]=='5008' and item[1]==item1[1] and item1[2]==item[2]:
                num_dy_download+=int(item1[3])
                item1[3]=0
        item[3]=0
        #print concatsql(day,hour,'1',item[1],item[2],'dy_download',str(num_dy_download))
        execmd(concatsql(day,hour,'1',item[1],item[2],'dy_download',str(num_dy_download)))


    elif item[0]=='5001' or item[0]=='5032':
        if item[3]==0:
            continue
        num_dy_del_task=int(item[3])
        for item1 in all_items:
            if item[0]=='5001' and item1[0]=='5032' and item[1]==item1[1] and item1[2]==item[2]:
                num_dy_del_task+=int(item1[3])
                item1[3]=0
            if item[0]=='5032' and item1[0]=='5001' and item[1]==item1[1] and item1[2]==item[2]:
                num_dy_del_task+=int(item1[3])
                item1[3]=0
        item[3]=0
        #print concatsql(day,hour,'1',item[1],item[2],'dy_del_task',str(num_dy_del_task))
        execmd(concatsql(day,hour,'1',item[1],item[2],'dy_del_task',str(num_dy_del_task)))


    elif item[0]=='5012' or item[0]=='1918':
        if item[3]==0:
            continue
        num_dy_finisth_download=int(item[3])
        for item1 in all_items:
            if item[0]=='5012' and item1[0]=='1918' and item[1]==item1[1] and item1[2]==item[2]:
                num_dy_finisth_download+=int(item1[3])
                item1[3]=0
            if item[0]=='1918' and item1[0]=='5012' and item[1]==item1[1] and item1[2]==item[2]:
                num_dy_finisth_download+=int(item1[3])
                item1[3]=0
        item[3]=0
        #print concatsql(day,hour,'1',item[1],item[2],'dy_finisth_download',str(num_dy_finisth_download))
        execmd(concatsql(day,hour,'1',item[1],item[2],'dy_finisth_download',str(num_dy_finisth_download)))

    elif item[0]=='5013' or item[0]=='1913' or item[0]=='5015':
        if item[3]==0:
            continue
        num_dy_install=int(item[3])
        for item1 in all_items:
            if item[0]=='5013' and (item1[0]=='5015' or item1[0]=='1913') and item[1]==item1[1] and item1[2]==item[2]:
                num_dy_install+=int(item1[3])
                item1[3]=0
            if item[0]=='5015' and (item1[0]=='5013' or item1[0]=='1913') and item[1]==item1[1] and item1[2]==item[2]:
                num_dy_install+=int(item1[3])
                item1[3]=0
            if item[0]=='1913' and (item1[0]=='5013' or item1[0]=='5015') and item[1]==item1[1] and item1[2]==item[2]:
                num_dy_install+=int(item1[3])
                item1[3]=0
        item[3]=0
        #print concatsql(day,hour,'1',item[1],item[2],'dy_install',str(num_dy_install))
        execmd(concatsql(day,hour,'1',item[1],item[2],'dy_install',str(num_dy_install)))
    
    elif item[0]=='5078' or item[0]=='5050' or item[0]=='5084':
        if item[3]==0:
            continue
        num_dy_install_v2=int(item[3])
        for item1 in all_items:
            if item[0]=='5078' and (item1[0]=='5050' or item1[0]=='5084') and item[1]==item1[1] and item1[2]==item[2]:
                print item1[3]
                num_dy_install_v2+=int(item1[3])
                item1[3]=0
            if item[0]=='5050' and (item1[0]=='5078' or item1[0]=='5084') and item[1]==item1[1] and item1[2]==item[2]:
                print item1[3]
                num_dy_install_v2+=int(item1[3])
                item1[3]=0
            if item[0]=='5084' and (item1[0]=='5078' or item1[0]=='5050') and item[1]==item1[1] and item1[2]==item[2]:
                print item1[3]
                num_dy_install_v2+=int(item1[3])
                item1[3]=0
        item[3]=0
        #print concatsql(day,hour,'2',item[1],item[2],'dy_install',str(num_dy_install_v2))
        execmd(concatsql(day,hour,'2',item[1],item[2],'dy_install',str(num_dy_install_v2)))








    

