package com.dev.elastic;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DataBaseFetcer {

	static Properties prop = new Properties();
	// Connect to your database.
	// Replace server name, username, and password with your credentials
	public static void main(String[] args) {

		try (InputStream input = new FileInputStream("../doc.properties")) {

			// load a properties file
			prop.load(input);

		} catch (IOException ex) {
			ex.printStackTrace();
		}

		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String connectionUrl = "jdbc:sqlserver://"+prop.getProperty("ur")+":1433;" + "database="+prop.getProperty("db")+";"
				+ "user="+prop.getProperty("ha")+";" + "password="+prop.getProperty("sh")+";";

		ResultSet resultSet = null;

		try (Connection connection = DriverManager.getConnection(connectionUrl);
				Statement statement = connection.createStatement();) {

			// Create and execute a SELECT SQL statement.
			String selectSql = "SELECT TOP 10 * from "+prop.getProperty("tb");
			resultSet = statement.executeQuery(selectSql);

			// Print results from select statement
			while (resultSet.next()) {
				System.out.println(resultSet.getString(2) + " " + resultSet.getString(3));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
