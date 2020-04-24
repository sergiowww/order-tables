package net.wicstech.ordertables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

import org.junit.jupiter.api.Test;

class OrderDeleteTest {

	@Test
	void testSqlServer() throws Exception {
		try (Connection connection = DriverManager.getConnection("jdbc:jtds:sqlserver://localhost/PROTETOR", "sa", "123456")) {
			OrderDelete order = new OrderDelete(connection);
			order.setSchemaPattern("dbo");
			List<String> tableNames = order.forDelete();
			tableNames.forEach(System.out::println);
			connection.setAutoCommit(false);
			connection.beginRequest();
			for (String tableName : tableNames) {
				try (PreparedStatement ps = connection.prepareStatement(String.format("delete from %s", tableName))) {
					int rows = ps.executeUpdate();
					System.out.printf("%d rows deleted%n", rows);
				} catch (Exception e) {
					connection.rollback();
					throw e;
				}
			}
			connection.rollback();
		}
	}
	
	@Test
	void testMysql() throws Exception {
		try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/study20191125", "root", "123")) {
			OrderDelete order = new OrderDelete(connection);
			List<String> tableNames = order.forDelete();
			System.out.println(tableNames);
			connection.setAutoCommit(false);
			connection.beginRequest();
			for (String tableName : tableNames) {
				try (PreparedStatement ps = connection.prepareStatement(String.format("delete from %s", tableName))) {
					int rows = ps.executeUpdate();
					System.out.printf("%d rows deleted%n", rows);
				} catch (Exception e) {
					connection.rollback();
					throw e;
				}
			}
			connection.rollback();
		}
	}

}
