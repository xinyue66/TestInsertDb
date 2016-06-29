#!/bin/sh
export JAVA_HOME=/opt/java1.6

PROCESSOR_NAME=insertDBTest

for jarfile in `ls lib/*.jar`
do
 export CLASSPATH=$CLASSPATH:$jarfile
done

FILE_OUT=./log/${PROCESSOR_NAME}.log

FILE_ERR=./log/${PROCESSOR_NAME}.err

FILE_PID=./log/${PROCESSOR_NAME}.pid

pid=`cat $FILE_PID | awk '{print $1}'`
if [ "${pid}" != "" ]
then
    kill -9  $pid
fi

echo `date` to start ... >>$FILE_OUT

$JAVA_HOME/bin/java -d64 -D$PROCESSOR_NAME -D"log4j.configuration=file:conf/log4j.properties" -Xms512m -Xmx512m -cp $CLASSPATH:insertDBTest.jar com.zznode.insertdb.Main $* 1>>$FILE_OUT 2>>$FILE_ERR &

echo $! >$FILE_PID
