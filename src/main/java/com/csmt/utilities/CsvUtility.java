package com.csmt.utilities;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;

public interface CsvUtility {

    public Map<String, String> getHeaderMap(Iterable<CSVRecord> records);

    public void updateTable(String filePath, String tableName, Connection connection) throws SQLException, IOException;
}
