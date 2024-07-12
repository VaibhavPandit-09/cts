package com.csmt.utilities.impl;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.csmt.io.CsvReader;
import com.csmt.utilities.CsvUtility;

public class CsvUtilityImpl implements CsvUtility {
    private static final int BATCH_SIZE = 50;

    @Override
    public Map<String, String> getHeaderMap(Iterable<CSVRecord> records) {
        Map<String, String> headerMap = new LinkedHashMap<String, String>();
        List<String> headers = new ArrayList<String>();
        List<String> dataTypes = new ArrayList<String>();

        int count = 0;
        for (CSVRecord record : records) {

            if (count == 0) {
                headers = record.toList();
                //System.out.println(headers.toString());
            }

            if (count == 1) {
                dataTypes = record.toList();
                //System.out.println(dataTypes.toString());
            }
            count++;
            if (count > 2) {
                break;
            }
        }
        for (int i = 0; i < headers.size(); i++) {
            headerMap.put(headers.get(i), dataTypes.get(i));
            //System.out.println(headers.get(i) + " : " + dataTypes.get(i));
        }

        return headerMap;
    }

    private String buildInsertSQL(String tableName, List<String> headers, int bulkCount) {
        StringBuilder insertSQL = new StringBuilder();
        insertSQL.append("INSERT INTO ").append(tableName).append(" (");
        for (int i = 0; i < headers.size(); i++) {
            insertSQL.append("[").append(headers.get(i)).append("]");
            if (i != headers.size() - 1) {
                insertSQL.append(", ");
            }
        }
        insertSQL.append(") VALUES ");
        for (int j = 0; j < bulkCount; j++) {
            insertSQL.append("(");
            for (int i = 0; i < headers.size(); i++) {
                insertSQL.append("?");
                if (i != headers.size() - 1) {
                    insertSQL.append(", ");
                }
            }
            insertSQL.append(")");
            if (j != bulkCount - 1) {
                insertSQL.append(", ");
            }
        }
        return insertSQL.toString();
    }


    @Override
    public void updateTable(String filePath, String tableName, Connection connection) throws SQLException, IOException {
        try (CSVParser parser = CsvReader.readCsvParse(filePath)) {
            List<String> headers = parser.getHeaderNames();

            // Map original headers to sanitized headers
            Map<String, String> headerMap = new HashMap<>();
            for (String header : headers) {
                headerMap.put(header.replace("/", "_"), header);
            }

            int count = 0;
            List<CSVRecord> batch = new ArrayList<>();
            for (CSVRecord record : parser) {
                
                if (count != 0) {
                    batch.add(record);
                }
                if (batch.size() >= BATCH_SIZE) {
                    insertBatch(batch, headerMap, tableName, connection);
                    batch.clear();
                }
                count++;
            }
            if (!batch.isEmpty()) {
                insertBatch(batch, headerMap, tableName, connection);
            }
        }
    }

    private void insertBatch(List<CSVRecord> batch, Map<String, String> headerMap, String tableName,
            Connection connection) throws SQLException {
        List<String> sanitizedHeaders = new ArrayList<>(headerMap.keySet());
        String insertSQL = buildInsertSQL(tableName, sanitizedHeaders, batch.size());
        //System.out.println("Executing SQL: " + insertSQL); // Debugging output
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
            int paramIndex = 1;
            for (CSVRecord record : batch) {
                for (String sanitizedHeader : sanitizedHeaders) {
                    String originalHeader = headerMap.get(sanitizedHeader);
                    String value = record.get(originalHeader).replace("'", "''").replace("/", "//"); // Properly escape
                                                                                                     // values
                    preparedStatement.setString(paramIndex++, value);
                }
            }
            preparedStatement.addBatch();
            preparedStatement.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println("Failed SQL: " + insertSQL); // Debugging output
        }
    }




    

}
