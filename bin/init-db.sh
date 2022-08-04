#!/bin/profile

host="localhost"
user='root'
password='123456'

if [ $# -eq 2 ]
then
  user=$1
  password=$2
elif [ $# -eq 1 ]; then
    user=$1
fi

file_path1=$(cd `dirname $0`; pwd)
file_path=`dirname ${file_path1}`
file_path=$file_path/src/sql/db_init.sql
echo $file_path
#echo ${file_path[0]}
#
#i=0
#echo ${#file_path}
#l=${#file_path}
#while($i < $l)
#do
#  echo ${file_path:$i}
#  echo "aaa"
#  $i++
#done

mysql -u$user -p$password -e "source ${file_path}"


