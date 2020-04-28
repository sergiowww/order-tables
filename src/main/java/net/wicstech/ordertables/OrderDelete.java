package net.wicstech.ordertables;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

	private String schemaPattern;
	private String tableNamePattern;
	private Map<String, List<String>> dependentTablesCache = new HashMap<>();


	public OrderDelete(Connection connection) {
		this.connection = connection;
	}

	private List<String> getTables() {
		try {
			List<String> tableNames = new ArrayList<>();
			try (ResultSet tables = getMetadata().getTables(this.getCatalog(), this.schemaPattern,
					this.tableNamePattern, null)) {
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
		System.out.printf("Inicial: %s%n", tables);
		for (; index < tables.size(); index = index + shifted + 1) {
			shifted = 0;
			try {
				String tableName = tables.get(index);
				System.out.printf("%d/%d -> %s (shifted: %d)%n", index, tables.size(), tableName, shifted);
				recurseTable(connection, tableName);
			} catch (SQLException e) {
				LOG.log(Level.SEVERE, e.getMessage(), e);
				throw new RuntimeException(e);
			}
		}
	}

	private void recurseTable(Connection connection, String tableName) throws SQLException {
		System.out.printf("%n================%n%s%n", tableName);
			List<String> depTables = getDependentTables(connection, tableName);
			for (String dependentTable : depTables) {
				int indiceEncontrado = tables.indexOf(dependentTable);
				int percorrido = index + shifted;
				System.out.printf("%s -> %s [found: %d] [%d + %d = %d]", tableName, depTables, indiceEncontrado, shifted, index, percorrido);
				if (indiceEncontrado > percorrido) {
					tables.add(index, tables.remove(indiceEncontrado));
					shifted++;
					System.out.printf(" %n%s", tables);
				}else if(indiceEncontrado >= 0 && indiceEncontrado < percorrido) {
					tables.add(index, tables.remove(indiceEncontrado));
					System.out.printf(" %n%s", tables);
				}

				recurseTable(connection, dependentTable);
			}
		
	}

	private List<String> getDependentTables(Connection connection, String tableName) throws SQLException {
		List<String> dependentTable = dependentTablesCache.get(tableName);
		if (dependentTable == null) {

			ResultSet rs = getMetadata().getImportedKeys(getCatalog(), "", tableName);
			List<Map<String, Object>> result = ResultSetUtil.extractResultSet(rs);
			dependentTable = result
					.stream()
					.map(t -> t.get("PKTABLE_NAME"))
					.map(String::valueOf)
					.distinct()
					.collect(Collectors.toList());
			dependentTablesCache.put(tableName, dependentTable);
		}
		return dependentTable;
	}

	public List<String> forDelete() {
		if (tables == null) {
			tables = getTables();
			sort();
			Collections.reverse(tables);
		}
		return tables;
	}

	public String getSchemaPattern() {
		return schemaPattern;
	}

	public void setSchemaPattern(String schemaPattern) {
		this.schemaPattern = schemaPattern;
	}

	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}

}
