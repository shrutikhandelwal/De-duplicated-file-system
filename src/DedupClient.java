import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class DedupClient {

	static final String SAVE_DIR = "./FROM_SERVER/";

	/**
	 * Segmentation and finger-printing of files should return a list of segment
	 * names (e.g. finger prints). A new file should be created that contains a
	 * list of segments to reconstruct the file.
	 */

	/**
	 * The metadata file contains information on the number of segments a file
	 * is split into and the segments that make up the file.
	 */
	public static HashSet<String> prepareSegments(File metadata) {
		HashSet<String> segments = new HashSet<>();
		try (BufferedReader br = new BufferedReader(new FileReader(metadata))) {
			String line;
			while ((line = br.readLine()) != null) {
				List<String> arr = Arrays.asList(line.split(","));
				segments.add(arr.get(0));
			}
		} catch (IOException e) {
			System.out.println(e);
		}
		return segments;
	}

	/**
	 * Input is a set of segments/finger prints. It communicates with EC2 to
	 * determine which need to be uploaded. Returns a set of segments that are
	 * unique and thus need to be uploaded.
	 */
	public static HashSet<String> getFilesToUpload(HashSet<String> segments, String address, int port) {
		HashSet<String> toUpload = new HashSet<String>();
		try (Socket socket = new Socket(address, port);
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
			out.println("duplicate check");
			System.out.println("Connected!");
			String input;
			for (String segment : segments) {
				out.println(segment);
				if ((input = in.readLine()) != null && input.equals("upload")) {
					toUpload.add(segment);
				}
			}
			in.close();
			out.close();
			socket.close();
		} catch (IOException e) {
			System.out.println(e);
			e.printStackTrace();
		}
		return toUpload;
	}

	/**
	 * This method sends the file to the server at the specified address and
	 * port.
	 * 
	 * @param file
	 * @param address
	 * @param port
	 */	
	public static void sendFile(File file, String address, int port) {
		try (Socket socket = new Socket(address, port);
				InputStream in = new FileInputStream(file);
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				OutputStream out = socket.getOutputStream();
				PrintWriter pw = new PrintWriter(out, true);
				) {
			long length = file.length();
			String uploadCmd = "upload " + file.getName() + " " + length + "\n";
			pw.println(uploadCmd);
			pause(1);
			String line;
			while ((line = br.readLine()) != null) {
				pw.println(line);
				pause(1);
			}
		}
		catch (IOException e) {
			System.out.println(e);
			e.printStackTrace();
		}
	}

	/**
	 * Returns an array of segments to reconstruct our file
	 */
	public static ArrayList<String> getFileSegments(String fileName, String address, int port) {
		try (Socket socket = new Socket(address, port);
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
			out.println("get " + fileName); // Tell server which file we want
			String input;
			input = in.readLine();
			if (input.endsWith(" does not exist!")) {
				System.err.println(fileName + " does not exist!");
				System.exit(1);
			} else {
				// Build our array of segments
				ArrayList<String> arr = new ArrayList<String>();
				String line;
				while ((line = in.readLine()) != null) {
					List<String> list = Arrays.asList(line.split(","));
					arr.add(list.get(0));
				}

				return arr;
			}
		} catch (IOException e) {
			System.out.println(e);
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Deletes the specified file from the server and S3 buckets
	 */
	public static void deleteFile(String fileName, String address, int port) {
		System.out.println("Deleting: " + fileName);
		try (Socket socket = new Socket(address, port);
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
			out.println("delete " + fileName);
			String input = in.readLine();
			if (input.equals("file does not exist!")) {
				System.out.println(fileName + " does not exist!");
			}
		} catch (IOException e) {
			System.out.println(e);
			e.printStackTrace();
		}
	}

	/**
	 * Compresses the specified file into fileName.zip
	 */
	public static void compressFile(String fileName) {
		File unzippedFile = new File(fileName);
		try (FileOutputStream fos = new FileOutputStream(fileName + ".zip");
				ZipOutputStream zos = new ZipOutputStream(fos);
				FileInputStream fis = new FileInputStream(new File(fileName))) {
			byte[] buffer = new byte[1];
			zos.putNextEntry(new ZipEntry(fileName));
			int length;
			while ((length = fis.read(buffer)) != -1) {
				zos.write(buffer, 0, length);
			}
			zos.closeEntry();
		} catch (IOException e) {
			System.out.println(e);
			e.printStackTrace();
		}
		File zippedFile = new File(fileName + ".zip");
		long unzippedLength = unzippedFile.length();
		long zippedLength = zippedFile.length();
		DecimalFormat format = new DecimalFormat("#.00");
	}

	/**
	 * Decompresses the file specified by zipFile to SAVE_DIR/fileName/saveName
	 * 
	 */
	public static void decompressFile(String fileName, String zipFile, String saveName) {
		// Save to SAVE_DIR + fileName + / + saveName
		// e.g. unzip file1's segments to ./FROM_SERVER/file1/saveName
		try (FileInputStream fis = new FileInputStream(zipFile); ZipInputStream zis = new ZipInputStream(fis);) {
			ZipEntry entry = zis.getNextEntry();
			byte[] buffer = new byte[1];
			File file = new File(SAVE_DIR + fileName + "/" + saveName);
			System.out.println("Unzipping to " + file.getPath());
			FileOutputStream fos = new FileOutputStream(file);
			int length;
			while ((length = zis.read(buffer)) != -1) {
				fos.write(buffer, 0, length);
			}
			fos.close();
			zis.closeEntry();
		} catch (IOException e) {
			System.out.println(e);
			e.printStackTrace();
		}
	}

	/**
	 * Rebuilds our file after obtaining segments from server/S3. The
	 * segments are deleted after the reconstruction.
	 */
	private static void reconstructFile(String fileName, ArrayList<String> segments) {
		try (PrintWriter pw = new PrintWriter(SAVE_DIR + fileName + "_deduped");) {
			System.out.println("File deduped to: " + SAVE_DIR + fileName + "_deduped");
			for (String segment : segments) {
				File seg = new File(SAVE_DIR + fileName + "/" + segment);
				FileInputStream fis = new FileInputStream(seg);
				byte[] fileContent = new byte[(int) seg.length()];
				fis.read(fileContent);
				fis.close();
				for (int i = 0; i < fileContent.length; i++) {
					pw.append((char) fileContent[i]);
				}
			}
			for (String segment : segments) {
				File seg = new File(SAVE_DIR + fileName + "/" + segment);
				seg.delete(); // delete unzipped segments
			}
		} catch (IOException e) {
			System.out.println(e);
			e.printStackTrace();
		}
	}

	private static void pause(int ms) {
		try {
			TimeUnit.MILLISECONDS.sleep(ms);
		} catch (InterruptedException e) {
			System.out.println(e);
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if (args.length != 4) {
			System.err.println("Usage: java DedupClient [put | get] [file] [address] [port]");
			System.exit(1);
		}
		// Collection information
		String command = args[0]; // put, get, or delete
		String myFile = args[1]; // file path or file name
		String address = args[2]; // server's address
		int port = Integer.parseInt(args[3]); // server's port
		ContentBasedChunking cbc = new ContentBasedChunking();

		if (command.equalsIgnoreCase("put")) {
			/*
			 * Step 1: Segment and finger print the files (output is metadata
			 * file)
			 */

			// File we want to upload
			File file = new File(myFile);

			// Directory containing our file, its metadata, and its segments
			String fileDir = file.getParent();
			System.out.println("fileDir: " + fileDir);

			try {
				cbc.digest(myFile);
				// myFile = /path/to/myFile
				// cbc.digest(myFile) creates /path/to/myFile.data
				// and /path/to/segments
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println(e);
			}
			System.out.println("Digest finished: " + myFile);
			// Metadata of file to be sent to server
			File metadata = new File(myFile + ".data"); // /path/to/myFile.data

			// Step 2: Determine segments of our file
			HashSet<String> segments = prepareSegments(metadata);

			// Step 3: Determine segments that need to be uploaded
			HashSet<String> toUpload = getFilesToUpload(segments, address, port);
			// Step 4: Send metadata file to the server
			System.out.println("Sending metadata!");
			sendFile(metadata, address, port);

			// Step 5: Initialize S3 client
			MyS3Client client = new MyS3Client();

			// Step 6: Send segments to buckets
			for (String upload : toUpload) {
				String segPath = fileDir + "/" + upload;
				File segFile = new File(segPath);
				// Compress the seg file to seg.zip
				compressFile(segPath);
				File segZip = new File(segPath + ".zip");
				client.uploadFile(segZip); // Upload zipped file
			}
			System.out.println("Number of segments actually uploaded: " + toUpload.size());
			// Delete segment files
			for (String seg : segments) {
				String segPath = fileDir + "/" + seg;
				File segFile = new File(segPath);
				File segZip = new File(segPath + ".zip");
				segFile.delete();
				segZip.delete();
			}
			metadata.delete(); // Delete file metadata
		}
		if (command.equalsIgnoreCase("get")) {
			File saveDir = new File(SAVE_DIR);
			if (!saveDir.exists()) {
				saveDir.mkdir();
			}
			// Step 1: Get the segments needed to reconstruct our file
			ArrayList<String> fileSegments = getFileSegments(myFile, address, port);
			HashSet<String> uniqueFiles = new HashSet<>(fileSegments);
			// Step 2: Initialize S3 client
			MyS3Client client = new MyS3Client();
			// Step 3: Download objects from S3
			client.downloadZippedSegments(myFile, uniqueFiles); // saves to
																// ./FROM_SERVER/filename/
			// Now we have a directory of .zip files: unzip and reconstruct the
			// file
			for (String zippedSegment : uniqueFiles) {
				String pathToZipSegment = SAVE_DIR + myFile + "/" + zippedSegment + ".zip";
				decompressFile(myFile, pathToZipSegment, zippedSegment);
				// Delete zip file
				new File(pathToZipSegment).delete();
			}
			reconstructFile(myFile, fileSegments);
			new File(SAVE_DIR + myFile).delete();
		}
		if (command.equalsIgnoreCase("delete")) {
			deleteFile(myFile, address, port);
		}
	}

}
