#!/bin/bash

DDD=`date --date="1 days ago" "+%Y%m%d"`

rsync -avP --password-file=/etc/rsync.pass.twin0589 /home/download/jianglinhe/ziyuanzhongxin/duanyou/result/duanyou_${DDD}.sql \
	xldc@10.1.1.189::TWIN_DATA/twin05a46/
rsync -avP --password-file=/etc/rsync.pass.twin0589 /home/download/jianglinhe/ziyuanzhongxin/xl79/result/xl7_${DDD}.sql \
	xldc@10.1.1.189::TWIN_DATA/twin05a46/
rsync -avP --password-file=/etc/rsync.pass.twin0589 /home/download/jianglinhe/ziyuanzhongxin/fanchuda/result/rbanner_${DDD}.sql 	 xldc@10.1.1.189::TWIN_DATA/twin05a46/
