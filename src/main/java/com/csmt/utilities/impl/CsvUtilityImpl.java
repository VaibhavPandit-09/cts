package com.csmt.utilities.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;

import com.csmt.utilities.CsvUtility;

public class CsvUtilityImpl implements CsvUtility {

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

    // @Override
    // public void updateTable(Iterable<CSVRecord> records, String tableName, Connection connection) throws SQLException {

    //     List<String> headers = new ArrayList<String>();
    //     List<String> data = new ArrayList<String>();

    //     int count = 0;
    //     for (CSVRecord record : records) {

    //         if (count == 0) {
    //             headers = record.toList();
    //             //System.out.println(headers.toString());
    //         }
    //         if (count > 1) {
    //             data = record.toList();
    //         }

    //         //Build add data in table table query using string builder
    //         StringBuilder addDataQuery = new StringBuilder();
    //         addDataQuery.append("INSERT INTO " + tableName + " (");
    //         for (int i = 0; i < headers.size(); i++) {
    //             addDataQuery.append(headers.get(i));
    //             if (i != headers.size() - 1) {
    //                 addDataQuery.append(", ");
    //             }
    //         }
    //         addDataQuery.append(") VALUES (");
    //         for (int i = 0; i < data.size(); i++) {
    //             addDataQuery.append("'" + data.get(i) + "'");
    //             if (i != data.size() - 1) {
    //                 addDataQuery.append(", ");
    //             }
    //         }
    //         addDataQuery.append(")");
    //         if (count == 2) {
    //             //System.out.println(headers.toString());
    //             System.out.println(addDataQuery.toString());
    //         }

    //         PreparedStatement statement = connection.prepareStatement(addDataQuery.toString());
    //         try {
    //             statement.executeUpdate();
    //         } catch (SQLException e) {

    //             e.fillInStackTrace();
    //         }

    //         count++;

    //     }

    // }

    // @Override
    // public void updateTable(Iterable<CSVRecord> records, String tableName, Connection connection) throws SQLException {
    //     List<String> headers = new ArrayList<String>();
    //     int count = 0;
    //     int batchSize = 10000; // Adjust batch size based on your memory constraints
    //     List<List<String>> batchData = new ArrayList<>();

    //     for (CSVRecord record : records) {
    //         if (count == 0) {
    //             headers = record.toList();
    //         } else if (count == 1) {
    //         } else {
    //             batchData.add(record.toList());
    //             if (batchData.size() >= batchSize) {
    //                 executeBatchInsert(batchData, headers, tableName, connection);
    //                 batchData.clear();
    //             }
    //         }
    //         count++;
    //     }

    //     // Insert any remaining records in the last batch
    //     if (!batchData.isEmpty()) {
    //         executeBatchInsert(batchData, headers, tableName, connection);
    //     }
    // }

    // private void executeBatchInsert(List<List<String>> batchData, List<String> headers, String tableName,
    //         Connection connection) throws SQLException {
    //     for (List<String> data : batchData) {
    //         StringBuilder addDataQuery = new StringBuilder();
    //         addDataQuery.append("INSERT INTO ").append(tableName).append(" (");

    //         for (int i = 0; i < headers.size(); i++) {
    //             addDataQuery.append(headers.get(i));
    //             if (i != headers.size() - 1) {
    //                 addDataQuery.append(", ");
    //             }
    //         }
    //         addDataQuery.append(") VALUES (");
    //         for (int i = 0; i < data.size(); i++) {
    //             // Escape single quotes by doubling them
    //             String escapedValue = data.get(i).replace("'", "''");
    //             addDataQuery.append("'").append(escapedValue).append("'");
    //             if (i != data.size() - 1) {
    //                 addDataQuery.append(", ");
    //             }
    //         }
    //         addDataQuery.append(")");

    //         PreparedStatement statement = connection.prepareStatement(addDataQuery.toString());
    //         try {
    //             statement.executeUpdate();
    //         } catch (SQLException e) {
    //             System.out.println(addDataQuery.toString());
    //             e.printStackTrace(); // Print stack trace for debugging
    //         } finally {
    //             if (statement != null) {
    //                 try {
    //                     statement.close();
    //                 } catch (SQLException e) {
    //                     e.printStackTrace();
    //                 }
    //             }
    //         }
    //     }
    // }

    // @Override
    // public void updateTable(Iterable<CSVRecord> records, String tableName, Connection connection) throws SQLException {
    //     List<String> headers = new ArrayList<>();
    //     int count = 0;
    //     int batchSize = 100; // Adjust batch size based on your memory constraints
    //     PreparedStatement preparedStatement = null;

    //     try {
    //         for (CSVRecord record : records) {
    //             if (count == 0) {
    //                 // Extract headers from the first record
    //                 headers = record.toList();
    //                 // Build the insert SQL statement dynamically
    //                 String insertSQL = buildInsertSQL(tableName, headers);
    //                 preparedStatement = connection.prepareStatement(insertSQL);
    //             }else if(count == 1){}
    //             else {
    //                 // Set the parameters for the prepared statement
    //                 for (int i = 0; i < headers.size(); i++) {
    //                     String escapedValue = record.get(i).replace("'", "''");
    //                     preparedStatement.setString(i + 1, escapedValue);
    //                 }
    //                 // Add to batch
    //                 preparedStatement.addBatch();

    //                 // Execute batch if batchSize is reached
    //                 if (++count % batchSize == 0) {
    //                     preparedStatement.executeBatch();
    //                 }
    //             }
    //             count++;
    //         }

    //         // Execute any remaining records in the last batch
    //         if (preparedStatement != null && count % batchSize != 0) {
    //             preparedStatement.executeBatch();
    //         }
    //     } catch (SQLException e) {
    //         e.printStackTrace(); // Print stack trace for debugging
    //     } finally {
    //         if (preparedStatement != null) {
    //             try {
    //                 preparedStatement.close();
    //             } catch (SQLException e) {
    //                 e.printStackTrace();
    //             }
    //         }
    //     }
    // }

    // private String buildInsertSQL(String tableName, List<String> headers) {
    //     StringBuilder insertSQL = new StringBuilder();
    //     insertSQL.append("INSERT INTO ").append(tableName).append(" (");
    //     for (int i = 0; i < headers.size(); i++) {
    //         insertSQL.append(headers.get(i));
    //         if (i != headers.size() - 1) {
    //             insertSQL.append(", ");
    //         }
    //     }
    //     insertSQL.append(") VALUES (");
    //     for (int i = 0; i < headers.size(); i++) {
    //         insertSQL.append("?");
    //         if (i != headers.size() - 1) {
    //             insertSQL.append(", ");
    //         }
    //     }
    //     insertSQL.append(")");
    //     return insertSQL.toString();
    // }

    // @Override
    // public void updateTable(Iterable<CSVRecord> records, String tableName, Connection connection) throws SQLException {
    //     List<String> headers = new ArrayList<>();
    //     int count = 0;
    //     int batchSize = 10000; // Adjust batch size based on your memory constraints
    //     PreparedStatement preparedStatement = null;

    //     try {
    //         for (CSVRecord record : records) {
    //             if (count == 0) {
    //                 // Extract headers from the first record
    //                 headers = record.toList();
    //                 // Build the insert SQL statement dynamically
    //                 String insertSQL = buildInsertSQL(tableName, headers);
    //                 preparedStatement = connection.prepareStatement(insertSQL);
    //             } else if (count == 1) {
    //                 // Skip the second row if needed
    //             } else {
    //                 // Set the parameters for the prepared statement
    //                 for (int i = 0; i < headers.size(); i++) {
    //                     String escapedValue = record.get(i).replace("'", "''");
    //                     preparedStatement.setString(i + 1, escapedValue);
    //                 }
    //                 // Add to batch
    //                 preparedStatement.addBatch();

    //                 // Execute batch if batchSize is reached
    //                 if (++count % batchSize == 0) {
    //                     preparedStatement.executeBatch();
    //                     System.out.println("Executed batch of size: " + batchSize);
    //                 }
    //             }
    //             count++;
    //         }

    //         // Execute any remaining records in the last batch
    //         if (preparedStatement != null && count % batchSize != 0) {
    //             preparedStatement.executeBatch();
    //             System.out.println("Executed final batch of size: " + (count % batchSize));
    //         }
    //     } catch (SQLException e) {
    //         e.printStackTrace(); // Print stack trace for debugging
    //         throw e;
    //     } finally {
    //         if (preparedStatement != null) {
    //             try {
    //                 preparedStatement.close();
    //             } catch (SQLException e) {
    //                 e.printStackTrace();
    //             }
    //         }
    //     }
    // }

    // private String buildInsertSQL(String tableName, List<String> headers) {
    //     StringBuilder insertSQL = new StringBuilder();
    //     insertSQL.append("INSERT INTO ").append(tableName).append(" (");
    //     for (int i = 0; i < headers.size(); i++) {
    //         insertSQL.append(headers.get(i));
    //         if (i != headers.size() - 1) {
    //             insertSQL.append(", ");
    //         }
    //     }
    //     insertSQL.append(") VALUES (");
    //     for (int i = 0; i < headers.size(); i++) {
    //         insertSQL.append("?");
    //         if (i != headers.size() - 1) {
    //             insertSQL.append(", ");
    //         }
    //     }
    //     insertSQL.append(")");
    //     return insertSQL.toString();
    // }

    // @Override
    // public void updateTable(Iterable<CSVRecord> records, String tableName, Connection connection) throws SQLException {
    //     List<String> headers = new ArrayList<>();
    //     int count = 0;
    //     int batchSize = 10000; // Adjust batch size based on your memory constraints
    //     PreparedStatement preparedStatement = null;

    //     try {
    //         for (CSVRecord record : records) {
    //             if (count == 0) {
    //                 // Extract headers from the first record
    //                 headers = record.toList();
    //                 // Build the insert SQL statement dynamically
    //                 String insertSQL = buildInsertSQL(tableName, headers);
    //                 preparedStatement = connection.prepareStatement(insertSQL);
    //             } else if (count == 1) {
    //                 // Skip the second row if needed
    //             } else {
    //                 // Set the parameters for the prepared statement
    //                 for (int i = 0; i < headers.size(); i++) {
    //                     String escapedValue = record.get(i).replace("'", "''");
    //                     preparedStatement.setString(i + 1, escapedValue);
    //                 }
    //                 // Add to batch
    //                 preparedStatement.addBatch();

    //                 // Execute batch if batchSize is reached
    //                 if (++count % batchSize == 0) {
    //                     preparedStatement.executeBatch();
    //                     System.out.println("Executed batch of size: " + batchSize);
    //                 }
    //             }
    //             count++;
    //         }

    //         // Execute any remaining records in the last batch
    //         if (preparedStatement != null && count % batchSize != 0) {
    //             preparedStatement.executeBatch();
    //             System.out.println("Executed final batch of size: " + (count % batchSize));
    //         }
    //     } catch (SQLException e) {
    //         e.printStackTrace(); // Print stack trace for debugging
    //         throw e;
    //     } finally {
    //         if (preparedStatement != null) {
    //             try {
    //                 preparedStatement.close();
    //             } catch (SQLException e) {
    //                 e.printStackTrace();
    //             }
    //         }
    //     }
    // }

    // private String buildInsertSQL(String tableName, List<String> headers, int bulkCount) {
    //     StringBuilder insertSQL = new StringBuilder();
    //     insertSQL.append("INSERT INTO ").append(tableName).append(" (");
    //     for (int i = 0; i < headers.size(); i++) {
    //         insertSQL.append(headers.get(i));
    //         if (i != headers.size() - 1) {
    //             insertSQL.append(", ");
    //         }
    //     }
    //     insertSQL.append(") VALUES");
    //     for (int j = 0; j < bulkCount; j++) {
    //         insertSQL.append(" (");
    //         for (int i = 0; i < headers.size(); i++) {
    //             insertSQL.append("?");
    //             if (i != headers.size() - 1) {
    //                 insertSQL.append(", ");
    //             }
    //         }
    //         insertSQL.append(")");
    //     }
    //     return insertSQL.toString();
    // }

    // @Override
    // public void updateTable(Iterable<CSVRecord> records, String tableName, Connection connection) throws SQLException {

    //     List<String> headers = new ArrayList<>();
    //     List<List<String>> batchDataList = new ArrayList<>();
    //     List<String> dataList = new ArrayList<>();
    //     PreparedStatement preparedStatement = null;
    //     int pBatchLength = 100;
    //     // Convert Iterable to List to get the total count
    //         List<CSVRecord> recordsList = new ArrayList<>();
    //         records.forEach(recordsList::add);
    //         int totalRecords = recordsList.size();

    //     int count = 0;
    //     int fBatchCount = 0;

    //     for (CSVRecord record : records) {

    //         if (count == 0) {
    //             headers = record.toList();
    //         }
    //         if (count > 1) {
    //             dataList = record.toList();
    //         }
    //         count++;
    //         if ((count != 0) && (count % pBatchLength == 0)) {
    //             // batchDataList.add(dataList);
    //             // dataList.clear();
    //             preparedStatement = connection
    //                     .prepareStatement(buildInsertSQL(tableName, headers, pBatchLength));
    //             // Set the parameters for the prepared statement
    //             for (int i = 0; i < pBatchLength; i++) {
    //                 String escapedValue = dataList.get(i).replace("'", "''");
    //                 preparedStatement.setString(i + 1, escapedValue);
    //             }
    //             preparedStatement.addBatch();

    //             fBatchCount++;
    //             if (fBatchCount == pBatchLength) {
    //                 fBatchCount = 0;
    //                 preparedStatement.executeBatch();
    //             }

    //         }
    //         int remainingRecords = totalRecords - (count + 1);

    //         if (remainingRecords < pBatchLength) {

    //             preparedStatement = connection
    //                     .prepareStatement(buildInsertSQL(tableName, headers, remainingRecords));
    //             // Set the parameters for the prepared statement
    //             for (int i = 0; i < pBatchLength; i++) {
    //                 String escapedValue = dataList.get(i).replace("'", "''");
    //                 preparedStatement.setString(i + 1, escapedValue);
    //             }
    //             preparedStatement.addBatch();
    //             preparedStatement.executeBatch();
    //             break;
    //         }
    //     }

    // }

    // private String buildInsertSQL(String tableName, List<String> headers, int bulkCount) {
    //     StringBuilder insertSQL = new StringBuilder();
    //     insertSQL.append("INSERT INTO ").append(tableName).append(" (");
    //     for (int i = 0; i < headers.size(); i++) {
    //         insertSQL.append(headers.get(i));
    //         if (i != headers.size() - 1) {
    //             insertSQL.append(", ");
    //         }
    //     }
    //     insertSQL.append(") VALUES");
    //     for (int j = 0; j < bulkCount; j++) {
    //         insertSQL.append(" (");
    //         for (int i = 0; i < headers.size(); i++) {
    //             insertSQL.append("?");
    //             if (i != headers.size() - 1) {
    //                 insertSQL.append(", ");
    //             }
    //         }
    //         insertSQL.append(")");
    //         if (j != bulkCount - 1) {
    //             insertSQL.append(",");
    //         }
    //     }
    //     return insertSQL.toString();
    // }

    // @Override
    // public void updateTable(Iterable<CSVRecord> records, String tableName, Connection connection) throws SQLException {
    //     List<String> headers = new ArrayList<>();
    //     List<String> dataList = new ArrayList<>();

    //     // Convert Iterable to List to get the total count
    //     List<CSVRecord> recordsList = new ArrayList<>();
    //     records.forEach(recordsList::add);
    //     int totalRecords = recordsList.size();

    //     if (totalRecords < 3)
    //         return; // No data to process

    //     int count = 0;

    //     // for (CSVRecord record : recordsList) {
    //     //     if (count == 0) {
    //     //         headers = record.toList();
    //     //     } else if (count != 1) { // Skip the second row
    //     //         dataList.addAll(record.toList());
    //     //     }
    //     //     count++;
    //     // }

    //     headers = recordsList.get(0).toList();

    //     // Calculate the maximum batch size
    //     int maxParams = 2100;
    //     int paramsPerRecord = headers.size();
    //     int maxBatchSize = maxParams / paramsPerRecord;

    //     for (int batchStart = 2; batchStart < totalRecords; batchStart += maxBatchSize) {
    //         int currentBatchSize = Math.min(maxBatchSize, totalRecords - batchStart);
    //         String insertSQL = buildInsertSQL(tableName, headers, currentBatchSize);
    //         try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
    //             for (int i = 0; i < currentBatchSize; i++) {
    //                 List<String> row = recordsList.get(batchStart + i).toList();
    //                 for (int j = 0; j < row.size(); j++) {
    //                     String escapedValue = row.get(j).replace("'", "''");
    //                     preparedStatement.setString(i * headers.size() + j + 1, escapedValue);
    //                 }
    //             }
    //             preparedStatement.addBatch();
    //             preparedStatement.executeBatch();
    //         }
    //     }
    // }








    private String buildInsertSQL(String tableName, List<String> headers, int bulkCount) {
        StringBuilder insertSQL = new StringBuilder();
        insertSQL.append("INSERT INTO ").append(tableName).append(" (");
        for (int i = 0; i < headers.size(); i++) {
            insertSQL.append(headers.get(i));
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
    public void updateTable(Iterable<CSVRecord> records, String tableName, Connection connection) throws SQLException {
        List<String> headers = new ArrayList<>();

        // Convert Iterable to List to get the total count
        List<CSVRecord> recordsList = new ArrayList<>();
        records.forEach(recordsList::add);
        int totalRecords = recordsList.size();

        if (totalRecords < 3)
            return; // No data to process

        headers = recordsList.get(0).toList(); // Get headers from the first row

        // Calculate the maximum batch size
        int maxParams = 2100;
        int paramsPerRecord = headers.size();
        int maxBatchSize = maxParams / paramsPerRecord;

        for (int i = 2; i < totalRecords; i += maxBatchSize) {
            int currentBatchSize = Math.min(maxBatchSize, totalRecords - i);
            String insertSQL = buildInsertSQL(tableName, headers, currentBatchSize);

            try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
                int paramIndex = 1;
                for (int j = i; j < i + currentBatchSize; j++) {
                    List<String> row = recordsList.get(j).toList();
                    for (String value : row) {
                        String escapedValue = value.replace("'", "''").replace("/", "//");
                        preparedStatement.setString(paramIndex++, escapedValue);
                    }
                }
                preparedStatement.addBatch();
                preparedStatement.executeBatch();
            }
        }
    }




    

}
