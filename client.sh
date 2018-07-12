#!/bin/bash +vx
LIB_PATH=$"/home/yaoliu/src_code/local/libthrift-1.0.0.jar:/home/yaoliu/src_code/local/slf4j-log4j12-1.5.8.jar:/home/yaoliu/src_code/local/slf4j-api-1.5.8.jar:/home/yaoliu/src_code/local/log4j-1.2.14.jar"

HOST=$1
PORT=$2
OPERATION="DefaultOperation"
FILENAME="DefaultFilename"
USER="DefaultUser"

while [[ $# -gt 1 ]]
do
key="$3"

case $key in
    -o|--operation)
    OPERATION="$4"
    shift # past argument
    ;;
    -f|--filename)
    FILENAME="$4"
    shift # past argument
    ;;
    -u|--user)
    USER="$4"
    shift # past argument
    ;;
    --default)
    DEFAULT=YES
    ;;
    *)
            # unknown option
    ;;
esac
shift # past argument or value
done

#hostname port operation filename user
java -classpath bin/client_classes:$LIB_PATH FileStoreClient ${HOST} ${PORT} ${OPERATION} ${FILENAME} ${USER}
