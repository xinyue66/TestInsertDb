#!/bin/sh
export JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk.x86_64

PROCESSOR_NAME=insertDBTest

for jarfile in `ls lib/*.jar`
do
 export CLASSPATH=$CLASSPATH:$jarfile
done

FILE_OUT=./log/${PROCESSOR_NAME}.log

FILE_ERR=./log/${PROCESSOR_NAME}.log

FILE_PID=./log/${PROCESSOR_NAME}.pid

pid=`cat $FILE_PID | awk '{print $1}'`
if [ "${pid}" != "" ]
then
    kill -9  $pid
fi
echo `date` to stop ... 
