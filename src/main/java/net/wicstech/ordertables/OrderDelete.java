package net.wicstech.ordertables;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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

	public void debugRs(ResultSet rs) throws SQLException {
		ResultSetMetaData me = rs.getMetaData();
		for (int i = 1; i <= me.getColumnCount(); i++) {
			String label = me.getColumnLabel(i);
			String value = rs.getString(i);
			System.out.printf("%s => %s%n", label, value);
		}
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
		List<String> foreignColumns = getForeignColumns(connection, tableName);
		for (String colName : foreignColumns) {
			String sql = readString("/find_table_by_fk.sql");
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, getCatalog() + "%");
			ps.setString(2, "%" + tableName);
			ps.setString(3, colName);

			List<Map<String, Object>> rs = ResultSetUtil.extractResultSet(ps);
			String dependentTable = rs
			//@formatter:off
					.stream()
					.flatMap(m -> m.values().stream())
					.findFirst()
					.map(String::valueOf)
					.map(s -> s.replaceFirst(catalog + "/", ""))
					.orElse(null);
				//@formatter:on

			int indiceEncontrado = tables.indexOf(dependentTable);
			if (indiceEncontrado > (index + shifted)) {
				tables.add(index, tables.remove(indiceEncontrado));
				shifted++;
			}
			recurseTable(connection, dependentTable);
		}
	}

	private String readString(String fileName) {
		try {
			return Files.readString(Paths.get(this.getClass().getResource(fileName).toURI()));
		} catch (IOException | URISyntaxException e) {
			LOG.log(Level.SEVERE, e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	private List<String> getForeignColumns(Connection connection, String tableName) throws SQLException {
		PreparedStatement ps = connection.prepareStatement(readString("/find_fk_by_table_name.sql"));
		ps.setString(1, getCatalog() + "%");
		ps.setString(2, "%" + tableName);
		List<Map<String, Object>> resultSet = ResultSetUtil.extractResultSet(ps);
		return resultSet.stream().map(m -> m.get("FOR_COL_NAME")).map(String::valueOf).collect(Collectors.toList());
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
