package com.exasol.aws_cost_reporting;

public class App {
	public static void main(String[] args) {
		// disable logging for http client
		System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
		
		// Do the import
		Importer importer = new Importer();
		importer.process();
        
		System.out.println("Goodbye!");
	}
}
