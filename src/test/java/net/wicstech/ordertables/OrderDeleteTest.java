package net.wicstech.ordertables;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.ScriptResolver;
import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.distribution.Version;

class OrderDeleteTest {

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
	void testMysqlStudy() throws Exception {
		runTestOn("study.sql", "study");
	}
	@Test
	void testMysqlSeverino() throws Exception {
		runTestOn("severino.sql", "severino");
	}

	private void runTestOn(String script, String schema) throws IOException, SQLException, Exception {
		MysqldConfig config = MysqldConfig.aMysqldConfig(Version.v5_7_latest).withFreePort()
				.build();

		EmbeddedMysql mysqld = EmbeddedMysql.anEmbeddedMysql(config)
				.addSchema(schema, ScriptResolver.classPathScript(script)).start();
		
		try (Connection connection = DriverManager.getConnection(String.format("jdbc:mysql://localhost:%d/%s", config.getPort(), schema), config.getUsername(),
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
