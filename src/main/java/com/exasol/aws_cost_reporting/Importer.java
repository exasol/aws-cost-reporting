/**
 * 
 */
package com.exasol.aws_cost_reporting;

import java.io.File;

import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * @author Marc.Popp@exasol.com
 *
 */
public class Importer {

	public S3Client s3client = new S3Client();
	public ExasolClient exasolclient = new ExasolClient();

	public void process() {
//		System.out.println(s3client.listBucket());
		for (S3ObjectSummary os : s3client.listBucket()) {
			if (os.getKey().endsWith("All-1.csv.gz")) {
				System.out.println("Processing " + os.getKey());
				this.processFile(os.getKey());
			} else {
				System.out.println("Skipping " + os.getKey());
			}
		}
	}

	private void processFile(String filename) {
		// Check import Status
		if (this.exasolclient.needsImport(filename)) {			
			// download
			String tempFile = this.s3client.fetchS3File(filename);

			// upload
			this.exasolclient.importCSV(tempFile, filename);

			// delete tempFile
			System.out.println("Deleting " + tempFile);
			File file = new File(tempFile);
			file.delete();
		}
		else {
			System.out.println("Skipping " + filename);
		}
	}

}
