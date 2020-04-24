package net.wicstech.ordertables;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Retrieve all tables from connection and order them for delete statements.
 * 
 * @author sergio
 *
 */
public class OrderDelete {
	private static final Logger LOG = Logger.getLogger(OrderDelete.class.getSimpleName());
	private final Connection connection;
	private String catalog;
	private DatabaseMetaData metadata;

	private List<String> tables;
	private int index;
	private int shifted;

	public OrderDelete(Connection connection) {
		this.connection = connection;
	}

	private List<String> getTables() {
		try {
			List<String> tableNames = new ArrayList<>();
			try (ResultSet tables = getMetadata().getTables(getCatalog(), "", "", null)) {
				while (tables.next()) {
					String table = tables.getString("TABLE_NAME");
					tableNames.add(table);
				}
			}
			return tableNames;
		} catch (SQLException e) {
			LOG.log(Level.SEVERE, e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	private DatabaseMetaData getMetadata() throws SQLException {
		if (this.metadata == null) {
			this.metadata = connection.getMetaData();
		}
		return metadata;
	}

	private String getCatalog() throws SQLException {
		if (this.catalog == null) {
			this.catalog = connection.getCatalog();
		}
		return this.catalog;
	}

	public void sort() {
		for (; index < tables.size(); index = index + shifted + 1) {
			shifted = 0;
			try {
				recurseTable(connection, tables.get(index));
			} catch (SQLException e) {
				LOG.log(Level.SEVERE, e.getMessage(), e);
				throw new RuntimeException(e);
			}
		}
	}

	private void recurseTable(Connection connection, String tableName) throws SQLException {
		List<String> foreignColumns = getDependentTables(connection, tableName);
		for (String dependentTable : foreignColumns) {

			int indiceEncontrado = tables.indexOf(dependentTable);
			if (indiceEncontrado > (index + shifted)) {
				tables.add(index, tables.remove(indiceEncontrado));
				shifted++;
			}
			recurseTable(connection, dependentTable);
		}
	}

	private List<String> getDependentTables(Connection connection, String tableName) throws SQLException {
		ResultSet rs = getMetadata().getImportedKeys(getCatalog(), "", tableName);
		List<Map<String, Object>> result = ResultSetUtil.extractResultSet(rs);
		return result
		//@formatter:off
				.stream()
				.map(t -> t.get("PKTABLE_NAME"))
				.map(String::valueOf)
				.collect(Collectors.toList());
		//@formatter:on
	}

	public List<String> forDelete() {
		if (tables == null) {
			tables = getTables();
			sort();
			Collections.reverse(tables);
		}
		return tables;
	}

}
