
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Scanner;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class FileStoreClient {

	private static BufferedOutputStream bufferedOutStream;

	private static TJSONProtocol jsonProtocol;

	public static void main(String[] args) {

		if (args.length > 2 && args.length <= 5) {

			String serverHostName = args[0];
			int serverPortNumber = Integer.parseInt(args[1]);

			try {
				TTransport transport;

				transport = new TSocket(serverHostName, serverPortNumber);
				transport.open();

				TProtocol protocol = new TBinaryProtocol(transport);                 // Uses TBinaryProtocol 
				FileStore.Client client = new FileStore.Client(protocol);

				if (args[2].contains("read")) {
					readOperation(client, args[3], args[4]);
				} else if (args[2].contains("write")) {
					writeOperation(client, args[3], args[4]);
				} else if (args[2].contains("list")) {
					listOperation(client, args[4]);
				}

				transport.close();
			} catch (ArrayIndexOutOfBoundsException e) {
				write(new SystemException().setMessage("Please enter valid arguments"));
			} catch (Exception x) {
				write(new SystemException().setMessage(x.toString()));
			}
		} else {
			write(new SystemException().setMessage("Invalid number of arguments"));
		}
	}

	/* 
	 * Calls "writeFile" operation on FileStore client for a 
	 * given (CONTENT,USER) and prints 
	 * the SUCCESS/FAILURE status using TJSONProtocol
	 *  */
	private static void writeOperation(FileStore.Client client, String fileName, String userName) {

		RFile createRfile = new RFile();

		RFileMetadata rfMeta = new RFileMetadata();

		rfMeta.setOwner(userName);
		rfMeta.setFilename(fileName);
		File inputFile = new File(fileName);
		createRfile.setMeta(rfMeta);

		Scanner scanner = null;
		String textContent = "";
		StatusReport storeResult = null;
		try {
			scanner = new Scanner(inputFile);
			textContent = scanner.useDelimiter("\\A").next();
			storeResult = client.writeFile(createRfile.setContent(textContent));
		} catch (FileNotFoundException e) {
			write(new SystemException().setMessage(e.toString()));
		} catch (SystemException e) {
			write(e);
		} catch (TException e) {
			write(new SystemException().setMessage(e.getMessage()));
		} finally {
			if(scanner != null)
			scanner.close();
		}

		write(storeResult);
	}

	/* 
	 * Calls "readFile" operation on FileStore client for a given
	 *  FILENAME, USER and prints 
	 *  File  contents and meta-information using TJSONProtocol
	 *  */
	private static void readOperation(FileStore.Client client, String fileName, String userName) {

		try {
			RFile returnFile = client.readFile(fileName, userName);
			write(returnFile);
		} catch (SystemException e) {

			write(e);
		} catch (TException e) {
			write(new SystemException().setMessage(e.toString()));
		}

	}

	/* 
	 * Calls "listOwnedFiles" operation on FileStore client for a given
	 *  USER and prints File  contents and meta-information 
	 *  using TJSONProtocol
	 *  */
	
	private static void listOperation(FileStore.Client client, String userName) {

		try {
			List<RFileMetadata> listOfFiles = client.listOwnedFiles(userName);
			write(listOfFiles);
		} catch (SystemException e) {
			write(e);
		} catch (TException e) {
			write(new SystemException().setMessage(e.toString()));
		}
	}

	
	/*
	 * Write method which uses TIOStreamTransport 
	 * and TJSONProtocol objects for writing results 
	 * of Operations(except list) to console in JSON format.
	 * */
	
	public static void write(TBase t) {
		if (t != null) {
			try {
				bufferedOutStream = new BufferedOutputStream(System.out, 2048);
				jsonProtocol = new TJSONProtocol(new TIOStreamTransport(bufferedOutStream));
				t.write(jsonProtocol);
				bufferedOutStream.flush();
				System.out.println("\n");
			} catch (Exception e) {
				write(new SystemException().setMessage(e.toString()));
			}
		}
	}
	
	/*
	 * Write method which uses TIOStreamTransport 
	 * , TJSONProtocol and TList objects for writing results 
	 * of listOwnedFiles operation to console in JSON format.
	 * */

	public static void write(List<RFileMetadata> t) {
		if (t != null) {
			try {

				bufferedOutStream = new BufferedOutputStream(System.out, 2048);
				jsonProtocol = new TJSONProtocol(new TIOStreamTransport(bufferedOutStream));
				jsonProtocol.writeListBegin(
						new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, t.size()));

				for (RFileMetadata _iter3 : t) {
					_iter3.write(jsonProtocol);
				}

				jsonProtocol.writeListEnd();
				bufferedOutStream.flush();
				System.out.println("\n");
			} catch (Exception e) {
				write(new SystemException().setMessage(e.toString()));
			}
		}
	}

}
