package com.exasol.aws_cost_reporting;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.util.List;

public class S3Client {

	protected String clientRegion = "eu-central-1";
	protected String bucketName = "bucketName";

	private BasicAWSCredentials awsCreds = new BasicAWSCredentials("xxxx",
			"xxx");
	private AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(clientRegion)
			.withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build();

	// List all Objects in the Bucket
	public List<S3ObjectSummary> listBucket() {
		ListObjectsV2Result result = s3Client.listObjectsV2(this.bucketName);
		List<S3ObjectSummary> objects = result.getObjectSummaries();		
		return objects;
	}
	
	// Fetch a file and store it locally as a temp file
	// returns the tempfile name
	public String fetchS3File(String filename) {

		S3Object fullObject = null;
		try {

			// Get an object and print its contents.
			System.out.println("Downloading an object");
			fullObject = this.s3Client.getObject(new GetObjectRequest(this.bucketName, filename));
			System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());
			return writeTempFile(fullObject.getObjectContent());
		} catch (AmazonServiceException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process
			// it, so it returned an error response.
			e.printStackTrace();
		} catch (SdkClientException e) {
			// Amazon S3 couldn't be contacted for a response, or the client
			// couldn't parse the response from Amazon S3.
			e.printStackTrace();
		} finally {
			// To ensure that the network connection doesn't remain open, close any open
			// input streams.
			if (fullObject != null) {
				try {
					fullObject.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	private static String writeTempFile(InputStream input) {
		try {
			// create a tmp file
			File tempFile = File.createTempFile("aws-cost-reporting-", ".tmp");
			System.out.println("Saving to tempfile: " + tempFile.getAbsolutePath());

			// write it
			BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
			BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(input)));
			String line = null;
			while ((line = reader.readLine()) != null) {
				writer.write(line + "\n");
			}
			writer.close();
			reader.close();
			System.out.println("Saved");
			return tempFile.getAbsolutePath();

		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public String getClientRegion() {
		return clientRegion;
	}

	public void setClientRegion(String clientRegion) {
		this.clientRegion = clientRegion;
	}

	public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}

}
