package net.wicstech.ordertables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * Generate delete statements in console.
 * 
 * Usage
 * <code> 
 * 
 * 		-DjdbcString=jdbc:mysql://localhost:3306/aurorabuzz-test?serverTimezone=America/Sao_Paulo
 * 		-DdbUsername=root
 * 		-DdbPassword=123456
 * 
 * </code>
 * 
 * @author sergio
 *
 */
public class GenerateDeleteStatements {

	public static void main(String[] args) throws SQLException {
		String jdbcString = System.getProperty("jdbcString");
		String username = System.getProperty("dbUsername");
		String password = System.getProperty("dbPassword");
		
		
		try (Connection connection = DriverManager.getConnection(jdbcString, username, password)) {
			OrderDelete order = new OrderDelete(connection);
			List<String> tableNames = order.forDelete();
			connection.setAutoCommit(false);
			for (String tableName : tableNames) {
				System.out.printf("delete from %s;%n", tableName);
					
			}
		}
	}

}
