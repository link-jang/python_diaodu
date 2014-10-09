#!/bin/bash
pid_array=($(ps aux|grep "get_data_hour.py"|grep -v "grep" | awk -F " " '{print $2}'))

for pid in ${pid_array[@]}
do
    {
                echo "kill -9 ${pid}"
                        kill -9 ${pid}
                    } &
done
wait
                
echo "stop finish!"

