package com.exasol.aws_cost_reporting;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ExasolClient {
	// JDBC driver name and database URL
	private String JDBC_DRIVER = "com.exasol.jdbc.EXADriver";
	private String DB_URL = "jdbc:exa:hosts:8563;schema=schema";

	// Database credentials
	private String USER = "xxx";
	private String PASS = "xxx";

	public boolean needsImport(String orginalFilename) {
		Connection conn = null;
		Statement stmt = null;
		try {
			// Open a connection
			conn = DriverManager.getConnection(this.DB_URL, this.USER, this.PASS);
			conn.setAutoCommit(false);

			// Execute a query
			stmt = conn.createStatement();
			String sql = "SELECT count(sessionId), filename FROM Imports GROUP BY filename HAVING filename = '" + orginalFilename + "';";
			ResultSet rs = stmt.executeQuery(sql);
			System.out.println(rs);

			boolean returnValue = true;
			rs.first();
			if (rs.first() && rs.getInt(1) > 0) {
				returnValue = false;
			}

			rs.close();

			return returnValue;
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			} // nothing we can do
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			} // end finally try
		} // end try
		return true;
	}

	public void importCSV(String filename, String orginalFilename) {
		Connection conn = null;
		Statement stmt = null;
		try {
			// Register JDBC driver
			Class.forName(this.JDBC_DRIVER);

			// Open a connection
			System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(this.DB_URL, this.USER, this.PASS);
			conn.setAutoCommit(false);

			// Execute a query
			System.out.println("Creating statement...");
			stmt = conn.createStatement();
			String sql;
			int importedRows;

			// Import the CSV file
			sql = "IMPORT INTO Costs_Staging " + "FROM LOCAL SECURE CSV " + "FILE '" + filename + "' "
					+ "(1, 2, 3, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 21, 24, 25, 129, 130, 131, 132) "
					+ "ENCODING = 'ASCII' " + "SKIP = 1 " + "COLUMN SEPARATOR = ',';";
			System.out.println("Executing Statement: " + sql);
			importedRows = stmt.executeUpdate(sql);
			System.out.println("Imported Rows: " + importedRows);

			// Turn staged data into production data
			sql = "INSERT INTO Costs\r\n" + "SELECT\r\n" + "    id,\r\n" + "    period,\r\n" + "    invoice,\r\n"
					+ "    payerAccount,\r\n" + "    TO_DATE(ISO_TS_TO_DATE(billingStart)),\r\n"
					+ "    TO_DATE(ISO_TS_TO_DATE(billingEnd)),\r\n" + "    usageAccount,\r\n" + "    costType,\r\n"
					+ "    TO_DATE(ISO_TS_TO_DATE(usageStart)),\r\n" + "    TO_DATE(ISO_TS_TO_DATE(usageEnd)),\r\n"
					+ "    awsProduct,\r\n" + "    operation,\r\n" + "    az,\r\n" + "    resource,\r\n"
					+ "    currency,\r\n" + "    blendedRate,\r\n" + "    blendedCost,\r\n" + "    createdBy,\r\n"
					+ "    department,\r\n" + "    owner,\r\n" + "    project\r\n" + "FROM Costs_Staging;";
			importedRows = stmt.executeUpdate(sql);
			System.out.println("Rows commited to prod: " + importedRows);

			// Empty the stage as preparation for the next import run
			sql = "TRUNCATE TABLE Costs_Staging;";
			importedRows = stmt.executeUpdate(sql);
			System.out.println("Rows removed from Staging: " + importedRows);

			// Record Import
			sql = "INSERT INTO Imports VALUES (CURRENT_SESSION, SYSTIMESTAMP, " + "'" + orginalFilename + "',"
					+ "'Import done.'" + ");";
			importedRows = stmt.executeUpdate(sql);
			System.out.println("Recorded input of file: " + orginalFilename);

			// Commit import
			conn.commit();

			// Close connections
			stmt.close();
			conn.close();
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			} // nothing we can do
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			} // end finally try
		} // end try
	}
}
