import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class MyS3Client {

	static final String[] BUCKETS = new String[] { "dedupbucket1", "dedupbucket2", "dedupbucket3" };
	static AmazonS3 client;

	public MyS3Client() {
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (/Users/<username>/.aws/credentials), and is in valid format.", e);
		}
		client = new AmazonS3Client(credentials);
	}

	/**
	 * Uploads the specified file to all of our S3 buckets
	 */
	public void uploadFile(File file) {
		try {
			System.out.println("Uploading: " + file.getName());
			for (String bucket: BUCKETS) {
				client.putObject(new PutObjectRequest(bucket, file.getName(), file));
			}
			
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which " + "means your request made it "
					+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which " + "means the client encountered "
					+ "an internal error while trying to " + "communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}
	
	/**
	 * Download the zipped segment files.
	 */
	public void downloadZippedSegments(String fileName, HashSet<String> segments) {
		int attempt = 0;
		boolean done = false;
		while (!done) {
			String s3Bucket = BUCKETS[attempt];
			String[] segmentArray = new String[segments.size()];
			segments.toArray(segmentArray);
			try {
				for (int i = 0; i < segmentArray.length; i++) {
					String segment = segmentArray[i];
					System.out.println("Downloading: " + segment + ".zip");
					S3Object s3Object = client.getObject(new GetObjectRequest(s3Bucket, segment + ".zip"));
					saveS3Object(fileName, s3Object);
					s3Object.close();
				}
				done = true;
			}
			catch (AmazonServiceException ase) {
				System.out.println("Caught an AmazonServiceException, which " + "means your request made it "
						+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
				System.out.println("Error Message:    " + ase.getMessage());
				System.out.println("HTTP Status Code: " + ase.getStatusCode());
				System.out.println("AWS Error Code:   " + ase.getErrorCode());
				System.out.println("Error Type:       " + ase.getErrorType());
				System.out.println("Request ID:       " + ase.getRequestId());
				if (attempt == 2) {
					// We failed!
					System.err.println("Failed to get segment in 3 attempts");
					System.exit(1);
				} else
					attempt++;
			} catch (AmazonClientException ace) {
				System.out.println("Caught an AmazonClientException, which " + "means the client encountered "
						+ "an internal error while trying to " + "communicate with S3, "
						+ "such as not being able to access the network.");
				System.out.println("Error Message: " + ace.getMessage());
				if (attempt == 2) {
					// We failed!
					System.err.println("Failed to get segment in 3 attempts");
					System.exit(1);
				} else
					attempt++;
			} catch (IOException e) {
				System.out.println(e);
			}
		}
	}
	
	/**
	 * Saves the s3Object that represents our segment file
	 */
	private static void saveS3Object(String fileName, S3Object s3Object) throws IOException {
		InputStream in = s3Object.getObjectContent();
		File fileDir = new File(DedupClient.SAVE_DIR + fileName);
		if (!fileDir.exists()) {
			fileDir.mkdir();
		}
		File zippedFile = new File(DedupClient.SAVE_DIR + fileName + "/" + s3Object.getKey());
		Files.copy(in, zippedFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
		in.close();
	}

	/**
	 * Deletes a segment from our s3 buckets
	 */
	public void deleteSegment(String segment) {
		try {
			System.out.println("Deleting: " + segment);
			for (String bucket: BUCKETS) {
				client.deleteObject(new DeleteObjectRequest(bucket, segment));
			}
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which " + "means your request made it "
					+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which " + "means the client encountered "
					+ "an internal error while trying to " + "communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

}
