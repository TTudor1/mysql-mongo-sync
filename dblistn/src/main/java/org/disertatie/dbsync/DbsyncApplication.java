package org.disertatie.dbsync;

import java.net.*;

// @SpringBootApplication
public class DbsyncApplication {
	public static void main(String[] args) {
		try {
			ServerSocket serverSocket = new ServerSocket(8082);
			System.out.println("Listening on port 8082...");
			while (true) {
				Socket clientSocket = serverSocket.accept();
				System.out.println("Connection from " + clientSocket.getInetAddress().getHostAddress());
				

				clientSocket.close();
			}
		} catch (Exception e) {
			System.out.println("Error listening on port 3306: " + e.getMessage());
		}
	}
}
