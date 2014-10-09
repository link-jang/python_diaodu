# -*- coding: utf-8 -*-
import sys
import os

mysql='/usr/local/mysql/bin/mysql -uroot -psd-9898w -N -e'
def execmd(cmd):
	pipe=os.popen('{ ' +cmd+'; } 2>> ../log/python.log')
	text=pipe.read()
	sts=pipe.close()

	if sts==None:
		sts=0
	if text[-1:] == '\n': text = text[:-1]
	return sts, text

if len(sys.argv)!=3:
	sys.exit(1)

day=sys.argv[1]
hour=sys.argv[2]

delete_sql=mysql+"\"use zyzx_data_static_db; Delete a from ftbl_duanyou_hour a, ftbl_duanyou_hour b where b.fday='"+day+"' and a.fhour='"+hour+"' and  b.fkey in('dy_valid_show','dy_all_show') and b.fvalue<50 and a.fday=b.fday and a.fhour=b.fhour and a.ftype=b.ftype and a.fversion=b.fversion and a.fgame=b.fgame; \""
execmd(delete_sql)
sql=mysql+" \"use zyzx_data_static_db;select * from ftbl_duanyou_hour where fday='"+day+"' and fhour='"+hour+"' \""
print sql
sts,text=execmd(sql)
#text=text.replace('\n','')
data=text.split('\n')

value=None
for item in data:
	item=item.split('\t')
	if value==None:
		value="('"+item[0]+"','"+item[1]+"','"+item[2]+"','"+item[3]+"','"+item[4]+"','"+item[5]+"','"+item[6]+"')"
	else:
		value=value+",('"+item[0]+"','"+item[1]+"','"+item[2]+"','"+item[3]+"','"+item[4]+"','"+item[5]+"','"+item[6]+"')"

exportsql='replace into support_stat.ftbl_duanyou_hour(fday,fhour,ftype,fversion,fgame,fkey,fvalue)values'+value
exportsql=exportsql+";delete from support_stat.ftbl_duanyou_hour where fversion in('7.9.2','7.9.3','7.99')"
#print exportsql
execmd('echo "'+exportsql +'" >>../result/fin_output/duanyou_'+day+'_'+hour+'.sql')
rsync='rsync -avP --password-file=/home/download/bin/rsync.pass.twin0589 ../result/fin_output/duanyou_'+day+'_'+hour+'.sql  xldc@10.1.1.189::TWIN_DATA/twin05a46/fin_output/'
print rsync
execmd(rsync)
