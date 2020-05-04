package net.wicstech.ordertables;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;

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

	private Graph<String, DefaultEdge> tree = new DefaultDirectedGraph<>(DefaultEdge.class);

	private List<String> tables;

	private String schemaPattern;
	private String tableNamePattern;

	public OrderDelete(Connection connection) {
		this.connection = connection;
	}

	private List<String> getTables() {
		try {
			List<String> tableNames = new ArrayList<>();
			try (ResultSet tables = getMetadata().getTables(this.getCatalog(), this.schemaPattern, this.tableNamePattern, null)) {
				while (tables.next()) {
					String table = tables.getString("TABLE_NAME");
					tableNames.add(table.toLowerCase());
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
		try {
			tables.forEach(tree::addVertex);
			for (String table : tables) {
				List<String> dependentTables = this.getDependentTables(table);
				dependentTables.forEach(d -> tree.addEdge(table, d));
			}
			TopologicalOrderIterator<String, DefaultEdge> depthFirstIterator = new TopologicalOrderIterator<>(tree);
			tables.clear();
			while(depthFirstIterator.hasNext()) {
				tables.add(depthFirstIterator.next());
			}
			System.out.printf("Final: %s%n", tables);	
		} catch (SQLException e) {
			LOG.log(Level.SEVERE, e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	private List<String> getDependentTables(String tableName) throws SQLException {
		ResultSet rs = getMetadata().getImportedKeys(getCatalog(), "", tableName);
		List<Map<String, Object>> result = ResultSetUtil.extractResultSet(rs);
		//@formatter:off
		return result
				.stream()
				.map(t -> t.get("PKTABLE_NAME"))
				.map(String::valueOf)
				.map(String::toLowerCase)
				.distinct()
				.collect(Collectors.toList());
		//@formatter:on
	}

	public List<String> forDelete() {
		if (tables == null) {
			tables = getTables();
			sort();
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
