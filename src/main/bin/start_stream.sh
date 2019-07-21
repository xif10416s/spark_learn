#!/bin/bash
SPARK_HOME='/data/cluster/spark/1.4.1/spark-1.4.1-bin-hadoop2.6'
cd `dirname $0`
BIN_DIR=`pwd`
cd ..
DEPLOY_DIR=`pwd`
CONF_DIR=$DEPLOY_DIR/conf
LOGS_DIR=$DEPLOY_DIR/logs
if [ ! -d $LOGS_DIR ]; then
    mkdir $LOGS_DIR
fi

TODAY=`date +"%F"`
STDOUT_FILE=$LOGS_DIR/stdout-$TODAY.log

LIB_DIR=$DEPLOY_DIR/lib
LIB_JARS=`ls $LIB_DIR|grep .jar|awk '{print "'$LIB_DIR'/"$0}'|tr "\n" ":"`

echo -e "Starting...\c $LIB_JARS"$LIB_DIR

nohup $SPARK_HOME/bin/spark-submit --class com.moneylocker.data.analysis.online.stream.ApiInvokeMonitor  --executor-memory 3g  --driver-class-path $DEPLOY_DIR/conf:$LIB_JARS   $LIB_DIR/stream-1.0-SNAPSHOT.jar $1 $2 $3 $4>> $STDOUT_FILE 2>&1 &
echo "OK!"
PIDS=`ps -f | grep java | grep "$DEPLOY_DIR" | awk '{print $2}'`
echo "PID: $PIDS"
echo "STDOUT: $STDOUT_FILE"
