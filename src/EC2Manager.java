import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupResult;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

/**
 * This class is in charge of starting up and
 * deleting EC2 instances
 */
public class EC2Manager {

	static final String SECURITY_GROUP = "Coen241DedupSecurityGroup";
	static final String SECURITY_GROUP_DESCRIPTION = "COEN241 Dedup Security Group";
	
	private static AmazonEC2 EC2Client;
	private static Set<Instance> instances = new HashSet<Instance>();

	
	private static void init() throws Exception {
		// Establish credentials
		AWSCredentials credentials = null;
		try {
		credentials = new ProfileCredentialsProvider("default").getCredentials();
		}
		catch (Exception e) {
			System.out.println(e);
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/Users/Johnny/.aws/credentials), and is in valid format.",
                    e);
		}
		EC2Client = new AmazonEC2Client(credentials);
		EC2Client.setEndpoint("ec2.us-west-2.amazonaws.com"); // Oregon
		createSecurityGroup();
	}
	
	private static void createSecurityGroup() {
		CreateSecurityGroupRequest csgRequest = new CreateSecurityGroupRequest();
		csgRequest.withGroupName(SECURITY_GROUP).withDescription(SECURITY_GROUP_DESCRIPTION);
		try {
		CreateSecurityGroupResult csgResult = EC2Client.createSecurityGroup(csgRequest);
		}
		catch (Exception e) {
			System.out.println(e);
			System.out.println("Security group will not be recreated!");
		}
		// Allow SSH from all IPs
		IpPermission ipPermission = new IpPermission();
		ipPermission.withIpRanges("0.0.0.0/0") // CIDR notation: /24 for IPv4 /32 for IPv6
					.withIpProtocol("tcp")
					.withFromPort(22)
					.withToPort(22);
		AuthorizeSecurityGroupIngressRequest asgiRequest = 
				new AuthorizeSecurityGroupIngressRequest();
		asgiRequest.withGroupName(SECURITY_GROUP)
				   .withIpPermissions(ipPermission);
		try {
		EC2Client.authorizeSecurityGroupIngress(asgiRequest);
		}
		catch (Exception e) {
			System.out.println(e);
			System.out.println("Ip permissions will not be recreated!");
		}
	}
	
	private static void launchEC2() {
		RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
		runInstancesRequest.withImageId("ami-16fd7026")
						   .withInstanceType("m1.micro")
						   .withMinCount(1)
						   .withMaxCount(1)
						   .withKeyName("Johnny")
						   .withSecurityGroups(SECURITY_GROUP);
		RunInstancesResult runInstancesResult = EC2Client.runInstances(runInstancesRequest);
	}
	
	private static void describeEC2() {
		DescribeInstancesResult describeResult = EC2Client.describeInstances();
		List<Reservation> reservations = describeResult.getReservations();
        for (Reservation reservation : reservations) {
        	instances.addAll(reservation.getInstances());
        }
        for (Instance instance : instances) {
        	System.out.println(instance.getState());
        	System.out.println("INSTANCE_ID: " + instance.getInstanceId());
        	System.out.println("PUBLIC_DNS_NAME: " + instance.getPublicDnsName());
        	System.out.println("PUBLIC_IP_ADDRESS: " + instance.getPublicIpAddress());
        }
        
	}

	public static void main(String[] args) throws Exception {
		init();
		launchEC2();
		describeEC2();
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Type 'stop' or 'terminate' stop or terminate this EC2 instance."); 
		while (true) {
			String input = br.readLine();
			if (input.equalsIgnoreCase("stop")) {
				StopInstancesRequest stopRequest = new StopInstancesRequest();
				for (Instance instance : instances) {
					stopRequest.withInstanceIds(instance.getInstanceId());
				}
				EC2Client.stopInstances(stopRequest);
				System.exit(0);
			}
			if (input.equalsIgnoreCase("terminate")) {
				TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest();
				for (Instance instance: instances) {
					terminateRequest.withInstanceIds(instance.getInstanceId());
				}
				EC2Client.terminateInstances(terminateRequest);
				System.exit(0);
			}
		}
	}
	
}
