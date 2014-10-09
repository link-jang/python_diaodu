#!/bin/bash
mysql46='/usr/local/mysql/bin/mysql -uroot -psd-9898w -N -e '

function _create_task_hour()
{
    day=$1
    sql="select count(*) from zyzx_data_static_db.ftbl_duanyou_schedule where fday='$day' "
    n=`$mysql46 "$sql"`
    if [[ $n -ne 24 ]];
    then
        i=0
        insert_sql="replace into zyzx_data_static_db.ftbl_duanyou_schedule(fday,fhour,fstatus)values($day,0,-3)"
        while [[ $i -lt 23 ]];
        do
            #echo $i
            ((i++))
            insert_sql=${insert_sql}",($day,$i,-3)"
            
        done
    fi

    echo $insert_sql
    $mysql46 "${insert_sql}"
}

day=`date -d "" +%Y%m%d`

function _protect_process()
{
    n=`ps aux|grep "get_data_hour1.py"|grep -v "grep"`
    if [[ x$n == x"" ]];
    then
        echo "the process is not exists ,please check it "
        sh start.sh
    fi
}



_create_task_hour $day
_protect_process
