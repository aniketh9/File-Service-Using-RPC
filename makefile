LIB_PATH=/home/yaoliu/src_code/local/libthrift-1.0.0.jar:/home/yaoliu/src_code/local/slf4j-log4j12-1.5.8.jar:/home/yaoliu/src_code/local/slf4j-api-1.5.8.jar
all: clean
	mkdir bin bin/client_classes bin/server_classes
	javac -classpath $(LIB_PATH) -d bin/client_classes/ src/FileStoreClient.java src/FileStoreHandler.java gen-java/* 
	javac -classpath $(LIB_PATH) -d bin/server_classes/ src/FileStoreServer.java src/FileStoreHandler.java gen-java/* 


clean:
	rm -rf bin/

