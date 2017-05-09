import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashSet;

public class EC2ServerThread extends Thread {

	private EC2Server server;
	private Socket socket;

	public EC2ServerThread(EC2Server server, Socket socket) {
		this.server = server;
		this.socket = socket;
	}

	/**
	 * The method that downloads the file the client is uploading.
	 */
	public static void receiveFile(String savePath, int fileLen, InputStream inputStream) {
		try (PrintWriter pw = new PrintWriter(savePath);
				InputStreamReader isr = new InputStreamReader(inputStream);
				BufferedReader in = new BufferedReader(isr);
				) {
			String line;
			while ((line = in.readLine()) != null) {
				pw.println(line);
			}
		}
		catch (IOException e) {
			e.printStackTrace();
			System.out.println(e);
		}
	}

	public void run() {
		// Runs when a connection is made
		try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));) {
			String input, output;
			input = in.readLine();
			// If client needs to determine which segments to upload
			if (input.equals("duplicate check")) {
				// We are expecting a series of "check segment" lines
				while ((input = in.readLine()) != null) {
					if (!server.hasSegment(input)) {
						out.println("upload");
					} else
						out.println("skip");
					server.putSegment(input);
				}
			}
			// If client is uploading a file
			if (input.startsWith("upload")) {
				String[] split = input.split(" ");
				String fileName = split[1];
				receiveFile(EC2Server.saveDest + fileName, Integer.parseInt(split[2]), socket.getInputStream());
				// ./SERVER/fileName
			}
			// If client wants to get a file
			if (input.startsWith("get")) {
				String[] split = input.split(" ");
				String fileName = split[1];
				File file = new File(EC2Server.saveDest + fileName + ".data");
				if (!file.exists()) {
					out.println("file: " + file.getName() + " does not exist!");
				} else {
					out.println("File exists");
					try (BufferedReader br = new BufferedReader(new FileReader(file))) {
						String line;
						while ((line = br.readLine()) != null) {
							out.println(line);
						}
					} catch (IOException e) {
						System.out.println(e);
						e.printStackTrace();
					}
				}
			}
			// If clients wants to delete a file
			if (input.startsWith("delete")) {
				String[] split = input.split(" ");
				String fileName = split[1];
				File file = new File(EC2Server.saveDest + fileName + ".data");
				if (!file.exists()) {
					out.println("file does not exist!");
				} else {
					out.println("deleting: " + fileName);
					System.out.println("Starting to delete: " + fileName);
					HashSet<String> uniqueSegments = DedupClient.prepareSegments(file);
					for (String seg : uniqueSegments) {
						server.removeSegment(seg);
					}
					if (file.delete()) {
						System.out.println("Deleted metadata file: " + file.getName());
					}
				}
			}
		} catch (IOException e) {
			System.out.println(e);
			e.printStackTrace();
		}
		EC2Server.saveMap();
		System.out.println("Operation done.");
	}

}
