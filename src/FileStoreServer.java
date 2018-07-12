import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.io.BufferedOutputStream;
import java.io.File;

public class FileStoreServer {

	private static BufferedOutputStream bufferedOutStreamServer;

	private static TJSONProtocol jsonProtocolServer;

	public static FileStoreHandler fileStoreHandler;

	public static FileStore.Processor filestoreProcessor;

	public static int port;
	
	/*
	 * "main" method that starts FileStore SERVER , 
	 * also creates a temp folder named "FileStore" for 
	 * storing files and their meta data
	 * */

	public static void main(String[] args) {
		try {
			
			fileStoreHandler = new FileStoreHandler();
			filestoreProcessor = new FileStore.Processor(fileStoreHandler);
			
			port = Integer.valueOf(args[0]);

			File newDir = new File("FileStore");
			deleteDir(newDir);
			newDir.mkdir();

			Runnable runnableSimple = new Runnable() {
				public void run() {
					simple(filestoreProcessor);
				}
			};

			new Thread(runnableSimple).start();

		} catch (Exception x) {
			write(new SystemException().setMessage(x.toString()));
		}

	}
	
    /*
     * Deletes if any previously created "FileStore"
     * folder and creates a new Folder every time the
     *  SERVER is restarted.
     *  */
	
	private static void deleteDir(File file) {
		File[] contents = file.listFiles();
		if (contents != null) {
			for (File f : contents) {
				deleteDir(f);
			}
		}
		file.delete();
	}
	
    /*
     * Method that fires up the server and 
     * gets things going
     * */
	public static void simple(FileStore.Processor processor) {
		try {
			TServerTransport serverTransport = new TServerSocket(port);
			TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));

			System.out.println("Starting the FileStore server...");
			server.serve();
		} catch (Exception e) {
			write(new SystemException().setMessage(e.toString()));
		}
	}
	
	/*
	 * Write method which uses TIOStreamTransport 
	 * and TJSONProtocol objects for writing exceptions
	 * to console in JSON format.
	 * */

	public static void write(TBase t) {
		try {
			bufferedOutStreamServer = new BufferedOutputStream(System.out, 2048);
			jsonProtocolServer = new TJSONProtocol(new TIOStreamTransport(bufferedOutStreamServer));
			t.write(jsonProtocolServer);

			bufferedOutStreamServer.flush();
			System.out.println("\n");
		} catch (Exception e) {
			write(new SystemException().setMessage(e.toString()));
		}
	}
}
