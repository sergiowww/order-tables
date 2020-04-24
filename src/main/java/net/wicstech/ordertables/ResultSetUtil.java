package net.wicstech.ordertables;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultSetUtil {
	public static List<Map<String, Object>> extractResultSet(Connection connection, String sql) throws Exception {
		PreparedStatement providedPS = connection.prepareStatement(sql);
		return extractResultSet(providedPS);

	}

	public static List<Map<String, Object>> extractResultSet(PreparedStatement providedPS) throws SQLException {
		List<Map<String, Object>> resultado = new ArrayList<>();
		try (PreparedStatement ps = providedPS; ResultSet rs = ps.executeQuery()) {
			while (rs.next()) {
				Map<String, Object> tupla = new HashMap<>();
				ResultSetMetaData metaData = rs.getMetaData();
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					String columnName = metaData.getColumnLabel(i);

					tupla.put(columnName, rs.getObject(columnName));
				}
				resultado.add(tupla);
			}
		}
		return resultado;
	}
}
