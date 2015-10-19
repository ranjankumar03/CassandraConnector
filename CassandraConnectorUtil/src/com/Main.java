package com;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

 /**
 * @author ranjan
 *
 */
public class Main {

	public static void main(String[] args) {

		final CassandraConnector client = new CassandraConnector();
		final String ipAddress = "localhost";
		final int port = 9042;
		System.out.println("Connecting to IP Address " + ipAddress + ":" + port + "...");
		client.connect(ipAddress, port);

		final String deleteQuery = "DELETE FROM hr.emp WHERE empid = ?";
		/*
		 * final String createQuery =
		 * "CREATE TABLE hr.emp (empid int primary key, empname varchar)";
		 * client.getSession().execute(createQuery);
		 */
		/*
		 * client.getSession().execute(
		 * "INSERT INTO hr.emp (empid, empname) VALUES (?, ?)",4,"ranjan");
		 */
		ResultSet results = client.getSession().execute("SELECT * FROM hr.emp WHERE empname='ranjan'");
		for (Row row : results) {
			System.out.format("%s %d\n", row.getString("empname"), row.getInt("e"));
		}

		client.getSession().execute(deleteQuery, 1);

		client.close();
		System.out.println("Terminating....");
		System.exit(0);

	}
}
