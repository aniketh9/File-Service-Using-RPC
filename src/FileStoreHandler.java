import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

public class FileStoreHandler implements FileStore.Iface {

	private BufferedOutputStream bufferedOutStream;

	private TJSONProtocol jsonProtocol;
	
	/* 
	 * Implements listOwnedFiles operation for a 
	 * given USER and returns list of RFileMetaData objects
	 * owned by USER
	 *  */

	public List<RFileMetadata> listOwnedFiles(String user) throws SystemException, TException {

		List<RFileMetadata> listownedFiles = new ArrayList<RFileMetadata>();
		boolean ifUserexists = false;
		RFileMetadata metData;
		File folder = new File("FileStore/");
		File[] listOfFiles = folder.listFiles();
		Path path1;
		for (int i = 0; i < listOfFiles.length; i++) {

			if (listOfFiles[i].isFile() && !listOfFiles[i].getName().contains("-meta")) {

				path1 = Paths.get("FileStore/" + listOfFiles[i].getName());

				try {
					metData = new RFileMetadata();
					BasicFileAttributes basicAttributes = Files.readAttributes(path1, BasicFileAttributes.class);
					String metaData = getMeta(listOfFiles[i].getName());
					String[] versionAndOwner = metaData.split(";");
					int versionValue = Integer.parseInt(versionAndOwner[0]);
					String ownerRetrieved = versionAndOwner[1];

					metData.setVersion(versionValue);
					metData.setOwner(ownerRetrieved);
					String[] fileValue = listOfFiles[i].getName().split("-");
					metData.setFilename(fileValue[0]);
					metData.setCreated(basicAttributes.creationTime().toMillis());
					metData.setUpdated(basicAttributes.lastModifiedTime().toMillis());
					metData.setContentLength((int) basicAttributes.size());
					metData.setContentHash(getMD5(listOfFiles[i]));

					if (ownerRetrieved.contentEquals(user)) {
						ifUserexists = true;
						listownedFiles.add(metData);
					}
				} catch (IOException e) {
					write(new SystemException().setMessage(e.toString()));
				}
			}
		}
		if (!ifUserexists) {
			SystemException se = new SystemException();
			se.setMessage("User " + user + " does not exist");
			throw se;
		}
		return listownedFiles;
	}

	/* 
	 * Implements writeFile operation for a 
	 * given FILENAME,USER,CONTENT and returns 
	 * SUCCESS OR FAILURE status
	 *  */
	
	public StatusReport writeFile(RFile rFile) throws SystemException, TException {

		StatusReport sReport = new StatusReport();
		sReport.setStatus(Status.FAILED);

		File newFile = new File("FileStore/" + rFile.meta.filename +"-"+ rFile.meta.owner);
		Writer writer = null;

		try {
			if (newFile.exists())
			 {if (!checkOwner(rFile.meta.filename+"-"+rFile.meta.owner, rFile.meta.owner)) {
				throw new SystemException()
						.setMessage("File " + rFile.meta.filename + " exists but it is not owned by " + rFile.meta.owner);
			    }
			 }
		} catch (IOException e1) {
			write(new SystemException().setMessage(e1.toString()));
		}
		try {
			
			if (newFile.exists()) {
				
				String metaData = getMeta(rFile.meta.filename+"-"+ rFile.meta.owner);
				String[] versionAndOwner = metaData.split(";");
				int value = Integer.parseInt(versionAndOwner[0]);

				writer = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream("FileStore/" + rFile.meta.filename+"-"+ rFile.meta.owner, false), "utf-8"));
				writer.write(rFile.getContent());

				createMetafile(value + 1, versionAndOwner[1], rFile.meta.filename+"-"+ rFile.meta.owner);
				sReport.setStatus(Status.SUCCESSFUL);

			} else {

				writer = new BufferedWriter(
						new OutputStreamWriter(new FileOutputStream("FileStore/" + rFile.meta.filename+"-"+ rFile.meta.owner), "utf-8"));
				writer.write(rFile.getContent());

				createMetafile(0, rFile.meta.owner, rFile.meta.filename+"-"+ rFile.meta.owner);
				sReport.setStatus(Status.SUCCESSFUL);
			}

		} catch (UnsupportedEncodingException e) {
			write(new SystemException().setMessage(e.toString()));
		} catch (FileNotFoundException e) {
			write(new SystemException().setMessage(e.toString()));
		} catch (IOException e1) {
			write(new SystemException().setMessage(e1.toString()));
		} catch (Exception e) {
			write(new SystemException().setMessage(e.toString()));
		} finally {
			try {
				writer.close();
			} catch (IOException e) {
				write(new SystemException().setMessage(e.toString()));
			}
		}

		return sReport;
	}
	
	/* 
	 * Implements readFile operation for a 
	 * given FILENAME,USER and returns 
	 * RFile object as output
	 *  */

	public RFile readFile(String filename, String owner) throws SystemException, TException {
		RFile responseRfile = new RFile();
		File inputFile = new File("FileStore/" + filename+"-"+owner);
		Path path1 = Paths.get("FileStore/" + filename+"-"+owner);
		RFileMetadata rfMeta = new RFileMetadata();
		Scanner scanner = null;

		if (inputFile.exists()) {

			try {
				if (!checkOwner(filename+"-"+owner, owner)) {
					throw new SystemException()
							.setMessage("File " + filename + " exists but it is not owned by " + owner);
				}
			} catch (IOException e1) {
				write(new SystemException().setMessage(e1.toString()));
			}

			try {
				String textContent = "";

				scanner = new Scanner(inputFile);
				textContent = scanner.useDelimiter("\\A").next();

				BasicFileAttributes basicAttributes = Files.readAttributes(path1, BasicFileAttributes.class);

				rfMeta.setOwner(owner);
				rfMeta.setFilename(filename);
				rfMeta.setCreated(basicAttributes.creationTime().toMillis());
				rfMeta.setUpdated(basicAttributes.lastModifiedTime().toMillis());
				rfMeta.setContentLength((int) basicAttributes.size());
				rfMeta.setContentHash(getMD5(inputFile));

				String metaData = getMeta(filename+"-"+owner);
				String[] versionAndOwner = metaData.split(";");
				int versionValue = Integer.parseInt(versionAndOwner[0]);

				rfMeta.setVersion(versionValue);

				responseRfile.setMeta(rfMeta);
				responseRfile.setContent(textContent);

			} catch (FileNotFoundException e) {
				write(new SystemException().setMessage(e.toString()));
			} catch (IOException e) {
				write(new SystemException().setMessage(e.toString()));

			} finally {
				scanner.close();
			}

		} else {
			throw new SystemException().setMessage("File: " + filename +" with Owner: "+ owner + " does not exist");
		}
		return responseRfile;
	}
	
	/*
	 * Boolean method that will check OWNERSHIP
	 * for given FILENAME and OWNER pair
	 * */

	private boolean checkOwner(String filename, String owner) throws IOException {

		String metaData = getMeta(filename);
		if (!metaData.isEmpty()) {
			String[] versionAndowner = metaData.split(";");
			if (versionAndowner[1].contentEquals(owner)) {
				return true;
			}
		}
		return false;
	}
    
	/* 
	 * For a given FILENAME this method will create 
	 * FILENAME-meta to store details of meta data
	 * fields
	 * */
	private void createMetafile(int version, String owner, String filename) {

		try {
			BufferedWriter writer = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream("FileStore/" + filename + "-meta"), "utf-8"));
			writer.write(version + ";" + owner);

			writer.close();
		} catch (IOException e) {
			write(new SystemException().setMessage(e.toString()));
		}

	}
	
	/*
	 * For a given FILENAME getMeta
	 * method will retrieve its meta fields
	 * VERSION and OWNER values.
	 *  */

	private String getMeta(String filename) {
		Scanner scanner = null;
		String textContent = "";
		File inputFile = new File("FileStore/" + filename + "-meta");

		try {
			scanner = new Scanner(inputFile);
			textContent = scanner.useDelimiter("\\A").next();
		} catch (Exception e) {
			write(new SystemException().setMessage(e.toString()));
		} finally {

			scanner.close();
		}

		return textContent;

	}
	
	/*
	 * For a given FILE its generates its 
	 * hex MD5 hash and returns it.
	 * */

	private String getMD5(File file) throws IOException {
		FileInputStream fis = new FileInputStream(file);
		StringBuilder stringBuilder = null;
		MessageDigest md5Digest;
		try {
			md5Digest = MessageDigest.getInstance("MD5");

			byte[] byteArray = new byte[1024];
			int bytesCount = 0;

			while ((bytesCount = fis.read(byteArray)) != -1) {
				md5Digest.update(byteArray, 0, bytesCount);
			}
			;

			fis.close();

			byte[] bytes = md5Digest.digest();

			stringBuilder = new StringBuilder();
			for (int i = 0; i < bytes.length; i++) {
				stringBuilder.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
			}
		} catch (NoSuchAlgorithmException e) {
			write(new SystemException().setMessage(e.toString()));
		}

		return stringBuilder.toString();
	}
	
	/*
	 * Write method which uses TIOStreamTransport 
	 * and TJSONProtocol objects for writing exceptions
	 * to console in JSON format.
	 * */

	public void write(TBase t) {
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
