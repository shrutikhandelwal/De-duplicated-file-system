import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.rabinfingerprint.fingerprint.RabinFingerprintLong;
import org.rabinfingerprint.fingerprint.RabinFingerprintLongWindowed;
import org.rabinfingerprint.handprint.BoundaryDetectors;
import org.rabinfingerprint.handprint.FingerFactory.ChunkBoundaryDetector;
import org.rabinfingerprint.polynomial.Polynomial;

public class ContentBasedChunking {

	static long bytesPerWindow = 24;
	static Polynomial p = Polynomial.createFromLong(10923124345206883L);
	private RabinFingerprintLong fingerHash = new RabinFingerprintLong(p);
	private RabinFingerprintLongWindowed fingerWindow = new RabinFingerprintLongWindowed(p, bytesPerWindow);
	private ChunkBoundaryDetector boundaryDetector = BoundaryDetectors.DEFAULT_BOUNDARY_DETECTOR;
	private final static int MIN_CHUNK_SIZE = 4096 / 4; // 1 KB 
	private final static int MAX_CHUNK_SIZE = 8192 * 4; // 32 KB
	private RabinFingerprintLong window = newWindowedFingerprint();
	
	public void storeSegmentInFile(String myFile, byte[] buf, int chunkLength, long fingerPrint) {
		try {
			File file = new File(myFile);
			String fileDir = file.getParent();
			Path p2 = Paths.get(fileDir + "/" + fingerPrint);
			OutputStream out = new BufferedOutputStream(Files.newOutputStream(p2));
			out.write(buf, 0, chunkLength);
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void digest (String myFile) throws IOException {
		long chunkStart = 0;
		long chunkEnd = 0;
		int chunkLength = 0;
		Path p1 = FileSystems.getDefault().getPath(myFile);
		byte[] barray = null;
		
		barray = Files.readAllBytes(p1);
		FileWriter fw = new FileWriter(myFile + ".data", true); //the true will append the new data
			
		ByteBuffer buf = ByteBuffer.allocateDirect(MAX_CHUNK_SIZE);
		buf.clear();
		/*
		 * fingerprint one byte at a time. we have to use this granularity to
		 * ensure that, for example, a one byte offset at the beginning of the
		 * file won't effect the chunk boundaries
		 */
		for (byte b : barray) {
			// push byte into fingerprints
			window.pushByte(b);
			fingerHash.pushByte(b);
			chunkEnd++;
			chunkLength++;
			buf.put(b);
			
			/*
			 * if we've reached a boundary (which we will at some probability
			 * based on the boundary pattern and the size of the fingerprint
			 * window), we store the current chunk fingerprint and reset the
			 * chunk fingerprinter.
			 */
			if (boundaryDetector.isBoundary(window)	&& chunkLength > MIN_CHUNK_SIZE) {
				byte[] c = new byte[chunkLength];
				buf.position(0);
				buf.get(c);
				
				/* Store fingerprint in metadata file */
				String s = fingerHash.getFingerprintLong() + "," + chunkLength + "\n";
				fw.write(s);
				
				storeSegmentInFile(myFile, c, chunkLength, fingerHash.getFingerprintLong());
				
				chunkStart = chunkEnd;
				chunkLength = 0;
				fingerHash.reset();
				buf.clear();
			} else if (chunkLength >= MAX_CHUNK_SIZE) {
				byte[] c = new byte[chunkLength];
				buf.position(0);
				buf.get(c);
				
				/* Store fingerprint in metadata file */
				String s = fingerHash.getFingerprintLong() + "," + chunkLength + "\n";
				fw.write(s);
				
				storeSegmentInFile(myFile, c, chunkLength, fingerHash.getFingerprintLong());
				
				fingerHash.reset();
				buf.clear();
				chunkStart = chunkEnd;
				chunkLength = 0;
			}
		}
		byte[] c = new byte[chunkLength];
		buf.position(0);
		buf.get(c);
		
		/* Store fingerprint in metadata file */
		String s = fingerHash.getFingerprintLong() + "," + chunkLength + "\n";
		fw.write(s);
		
		storeSegmentInFile(myFile, c, chunkLength, fingerHash.getFingerprintLong());
		fingerHash.reset();
		buf.clear();
		fw.close();
	}

	private RabinFingerprintLongWindowed newWindowedFingerprint() {
		return new RabinFingerprintLongWindowed(fingerWindow);
	}
}
