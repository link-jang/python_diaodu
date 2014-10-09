#!/bin/bash

[ -z ${EXP_DATE} ] && EXP_DATE="$1"
. /home/download/lixuanhao/env_init/hive.env

cd `dirname $0`
config="set hive.exec.compress.output=true;
set mapred.output.compress=true;
set mapred.compress.map.output=true;
set mapred.reduce.tasks=8;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec"

Mysql="/usr/local/mysql/bin/mysql -uroot -psd-9898w -N -e "
id_all="(cast(id as int)>=1901 and cast(id as int)<=1920 or cast(id as int)>=5001 and cast(id as int)<=5035 ) or id='5045'"
id_json="'1907','1909','1910','1913','1918','5001','5002','5008','5012','5013','5014','5015','5016','5032','5033','5084','5079','5078','5077','5068','5067','5060','5059','5058','5056','5055','5054','5051','5050','5048','5057','5047'"
id_click="'1901','1902''1903','1904','1905','1906','1908','1911','1912','1915','1916','1917','5003','5004','5005','5006','5007','5009','5010','5011','5049','5083','5082','5081','5080','5069','5066','5065','5064','5063','5062','5061','5053','5052','5049','5046','5045','5075','5074'"
value1_arr="cast(value1 as int)>=6 and cast(value1 as int)<=1000 and value1 rlike '^[0-9]+$' "



if [ $# -ne 2 ]
then
    echo "input date hour "
    exit 1
fi

day=$1
hour=$2

if [[ `expr length  "$day"` -ne 8 ]];
then
    echo 'input error day'
    exit 1
fi


if [ $hour -lt 0  -o  $hour -gt 23 ];
then
    echo "input error hour,hour is betwin 0-23"
    exit 1
fi


function _insert_db()
{
	day=$1
	hour=$2
    insert_sql="$config;
	insert overwrite table download_mid.duanyou_data partition(ds='$day',dt)
	select peerid,id,value1,version,value2,dt from 
	(
	select substring(peerid,0,15) as peerid,id,value1,value2 ,substring(product_ver,0,locate('.',product_ver,5)-1) as version ,'${hour}json' as dt
	from download_odl.stat_xl_79_new_json_json 
	where  ds='$day' and cast(hour as int)=$hour and $value1_arr and id in($id_json)  and product_ver rlike '.*7.9.*'
	union all
    
	select substring(peerid,0,15) as peerid,id,'' as value1 ,'' as value2 ,substring(product_ver,0,locate('.',product_ver,5)-1) as version ,'${hour}click' as dt
	from download_odl.stat_xl_79_new_json_num
	where  ds='$day' and cast(hour as int)=$hour and id in($id_click) and product_ver rlike '.*7.9.*'

	)t
	"
	
	
	echo "$insert_sql"
	$HIVE "$insert_sql"
}

function _close()
{
	day=$1
    hour=$2
	close_sql="
		select count(*) from download_mid.duanyou_data
		where ds='$day' and dt='${hour}json' and id in('1910','5048')		
	"

	num_json=`$HIVE "$close_sql"`
	close_click_sql="
		select count(*) from download_mid.duanyou_data
		where ds='$day' and dt='${hour}click' and id='5005' 
	"
	#num_click=`$HIVE "$close_click_sql"`

	let "num=$num_json+$num_click"

	$Mysql "replace into zyzx_data_static_db.ftbl_duanyou_hour(fday,fhour,ftype,fversion,fgame,fkey,fvalue)value('$day',$hour,'1','','','dy_close',$num_json);"
	echo $day $hour $num >>../result/value_close.txt
	
}

function _output_json()
{
	#json  数据
	day=$1
    hour=$2
	rm ../result/mid_output/${day}/value_json_${day}_${hour}.txt
	json_sql="
		select id,value1,version,count(*) from download_mid.duanyou_data where
		ds='$day' and dt='${hour}json' group by id,value1,version
	"
	
	echo $json_sql
	$HIVE "$json_sql">>../result/mid_output/${day}/value_json_${day}_${hour}.txt
}	


function _ountput_click()
{
	#click 数据
	day=$1
    hour=$2
	rm ../result/mid_output/${day}/value_click_${day}_${hour}.txt
	click_sql="
	    select id,'',version,count(*) from download_mid.duanyou_data where
		ds='$day' and dt='${hour}click' group by id,version
	 "
	echo $click_sql
	$HIVE "$click_sql">>../result/mid_output/${day}/value_click_${day}_${hour}.txt
	
}

function _pydeal()
{
	#导入数据库
	day=$1
    hour=$2
	python dealFile.py  "../result/mid_output/${day}/value_json_${day}_${hour}.txt" "$day" "$hour"

}


function _output_user()
{
	day=$1
    hour=$2
	rm ../result/mid_output/${day}/user_${day}_${hour}.txt
	user_game_sql="
	select id,value1,version,count(distinct peerid) from download_mid.duanyou_data where
	ds='$day' and dt='${hour}json' and id in ('1907','5054')  group by id,value1,version;
	"
	echo $user_game_sql
	$HIVE "$user_game_sql"|
	while read id_num value1_num version_num count_num
	do
        if [ x"$id_num" == "x1907" ];
        then
		    $Mysql "replace into zyzx_data_static_db.ftbl_duanyou_hour(fday,fhour,ftype,fversion,fgame,fkey,fvalue)value('$day',${hour},'1','$version_num','$value1_num','dy_game_users',$count_num);"
        elif [ x"$id_num" == "x5054" ];
        then
            $Mysql "replace into zyzx_data_static_db.ftbl_duanyou_hour(fday,fhour,ftype,fversion,fgame,fkey,fvalue)value('$day',${hour},'2','$version_num','$value1_num','dy_game_users',$count_num);"
        echo $value1_num $version_num $count_num >>../result/mid_output/${day}/user_${day}_${hour}.txt
	    fi
    done
	
}



function _export_data()
{
	day=$1
    hour=$2
    rm ../result/fin_output/duanyou_${day}_${hour}.sql
	python exportSql_duanyou.py $day $hour

}




_insert_db $day $hour

ret=$?
if [ $ret -ne 0 ];then
	exit 1
fi
mkdir -p ../result/mid_output/${day}/

_output_json $day $hour
ret=$?
if [ $ret -ne 0 ];then
    exit 1
fi
_ountput_click $day $hour
_pydeal $day $hour
_output_user $day $hour

_export_data $day $hour


cd -
