import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Intended to be run on the EC2 instance
 */
public class EC2Server {

	static final String saveDest = "./SERVER/";

	private ServerSocket serverSocket;
	private String address;
	private int port;

	private volatile static HashMap<String, Integer> segmentToCount = null;
	private ExecutorService threadPool;

	public EC2Server() {
		try {
			serverSocket = new ServerSocket(0); // automatically find a port
			port = serverSocket.getLocalPort();
			address = serverSocket.getInetAddress().getHostAddress();
		} catch (IOException e) {
			System.out.println(e);
		}
		System.out.println("Server is up: " + address + " : " + port);
		segmentToCount = loadMap(); // Load up the mappings when this runs
		printSegments(); // Uncomment this if you wish to see what segments get
							// loaded when server starts
		threadPool = Executors.newCachedThreadPool();
	}

	/**
	 * Debug purposes, prints the set of keys after loading serialized
	 * hash file.
	 */
	private static void printSegments() {
		for (String key : segmentToCount.keySet()) {
			System.out.println(key + ": " + segmentToCount.get(key));
		}
	}

	// Save our mappings after each thread halts
	public static void saveMap() {
		if (segmentToCount != null) {
			File mapFile = new File(saveDest + "segmentMapping.ser");
			try (FileOutputStream fileOut = new FileOutputStream(mapFile);
					ObjectOutputStream objectOut = new ObjectOutputStream(fileOut)) {
				objectOut.writeObject(segmentToCount);
			} catch (IOException e) {
				System.out.println(e);
			}
		}
	}

	// Loads our mappings
	private synchronized static HashMap<String, Integer> loadMap() {
		HashMap<String, Integer> map = null;
		File mapFile = new File(saveDest + "segmentMapping.ser");
		if (mapFile.exists()) {
			try (FileInputStream fileIn = new FileInputStream(mapFile);
					ObjectInputStream objectIn = new ObjectInputStream(fileIn)) {
				map = (HashMap<String, Integer>) objectIn.readObject();
				return map;
			} catch (IOException e) {
				System.out.println(e);
			} catch (ClassNotFoundException e) {
				System.out.println(e);
			}
		}
		return new HashMap<String, Integer>();
	}

	// String segment is the segment file
	public synchronized void putSegment(String segment) {
		if (segmentToCount.containsKey(segment)) {
			int count = segmentToCount.get(segment);
			segmentToCount.put(segment, count + 1);
		} else
			segmentToCount.put(segment, 1);
	}

	public synchronized void removeSegment(String segment) {
		if (segmentToCount.containsKey(segment)) {
			int count = segmentToCount.get(segment);
			if (count == 1) {
				// delete this segment
				deleteSegment(segment);
			} else
				segmentToCount.put(segment, count - 1);
		}
	}

	private synchronized void deleteSegment(String segment) {
		segmentToCount.remove(segment); // remove the segment
		MyS3Client client = new MyS3Client();
		// Delete objects from S3
		client.deleteSegment(segment + ".zip");
	}

	public synchronized boolean hasSegment(String segment) {
		return segmentToCount.containsKey(segment);
	}

	public static void main(String[] args) {
		File file = new File(saveDest);
		if (!file.exists()) {
			file.mkdir();
		}
		EC2Server ec2Server = new EC2Server();
		try (ServerSocket serverSocket = ec2Server.serverSocket) {
			while (true) {
				ec2Server.threadPool.submit(new EC2ServerThread(ec2Server, serverSocket.accept()));
			}
		} catch (IOException e) {
			System.out.println(e);
		}
	}
}
