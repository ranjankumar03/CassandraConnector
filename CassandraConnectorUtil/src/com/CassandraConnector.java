package com;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

 /**
 * @author ranjan
 *
 */
public class CassandraConnector {

	/** Cassandra Cluster. */
	private Cluster cluster;
	/** Cassandra Session will manage the connections to our cluster */
	private Session session;

	/**
	 * Connect to Cassandra Cluster specified by provided node IP address and
	 * port number.
	 *
	 * @param node
	 *            Cluster node IP address.
	 * @param port
	 *            Port of cluster host.
	 */
	public void connect(final String node, final int port) {
		/*
		 * Connect to your instance using the Cluster.builder method. It will
		 * add a contact point and build a cluster instance.
		 **/
		this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		Metadata metadata = cluster.getMetadata();

		System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());

		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(),
					host.getRack());
		}
		/* Get a session from your cluster, connecting to the “hr” keyspace */
		session = cluster.connect("hr");
	}

	/**
	 * Provide my Session.
	 *
	 * @return My session.
	 */
	public Session getSession() {
		return this.session;
	}

	/** Close cluster. */
	public void close() {
		cluster.close();
	}
}
