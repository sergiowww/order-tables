package net.wicstech.ordertables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.ScriptResolver;
import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.distribution.Version;

class OrderDeleteTest {

	private static final String SCHEMA_DATABASE = "severino";

	@Test
	@Disabled
	void testSqlServer() throws Exception {
		try (Connection connection = DriverManager.getConnection("jdbc:jtds:sqlserver://localhost/PROTETOR", "sa",
				"123456")) {
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
		MysqldConfig config = MysqldConfig.aMysqldConfig(Version.v5_7_latest).withFreePort()
				.build();

		EmbeddedMysql mysqld = EmbeddedMysql.anEmbeddedMysql(config)
				.addSchema(SCHEMA_DATABASE, ScriptResolver.classPathScript("severino.sql")).start();
		
		try (Connection connection = DriverManager.getConnection(String.format("jdbc:mysql://localhost:%d/%s", config.getPort(), SCHEMA_DATABASE), config.getUsername(),
				config.getPassword())) {
			OrderDelete order = new OrderDelete(connection);
			List<String> tableNames = order.forDelete();
			System.out.println(tableNames);
			connection.setAutoCommit(false);
			connection.beginRequest();
			for (String tableName : tableNames) {
				String deleteCommand = String.format("delete from %s", tableName);
				try (PreparedStatement ps = connection.prepareStatement(deleteCommand)) {
					System.out.printf("%s -> ...", deleteCommand);
					int rows = ps.executeUpdate();
					System.out.printf("%d rows deleted%n", rows);
				} catch (Exception e) {
					connection.rollback();
					throw e;
				}
			}
			connection.rollback();
		} finally {
			mysqld.stop();
		}

	}

}
