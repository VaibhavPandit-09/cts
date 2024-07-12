package com.csmt;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.csv.CSVRecord;

import com.csmt.database.DatabaseConnection;
import com.csmt.database.impl.DatabaseConnectionImpl;
import com.csmt.io.CsvReader;
import com.csmt.utilities.CsvUtility;
import com.csmt.utilities.impl.CsvUtilityImpl;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException, SQLException
    {
        // String path = "csv/match_data.csv";
        String path = "csv/Books_rating.csv";
        String name = CsvReader.getCsvName(path);

        Iterable<CSVRecord> records = CsvReader.readCsv(path);
        CsvUtility csvUtility = new CsvUtilityImpl();
        Map<String, String> headerMap = csvUtility.getHeaderMap(records);
        //System.out.println(headerMap.toString());
        
        //Create prepared string with String Builder and the map to create table in db
        StringBuilder createTableQuery = new StringBuilder("CREATE TABLE " + name + " (");
        

        for (Map.Entry<String, String> entry : headerMap.entrySet()) {
            String key = entry.getKey();
            key = key.replace("/", "_");
            String value = entry.getValue();
            value = value.replace("/", "_");
            createTableQuery.append(key).append(" ").append(value).append(", ");
        }
                        
        createTableQuery.deleteCharAt(createTableQuery.length() - 2);
        createTableQuery.append(")");
        
        //System.out.println(createTableQuery.toString());


        DatabaseConnection databaseConnection = new DatabaseConnectionImpl();

        Connection connection = databaseConnection.getConnection();

        PreparedStatement statement = connection.prepareStatement(createTableQuery.toString());
        try {
            statement.executeUpdate();
            
        } catch (Exception e) {
            System.out.println(createTableQuery.toString());
            e.printStackTrace();
        }
        

        //Calculate time to execute for the following 2 calls in minutes seconds and miliseconds
        long startTime = System.currentTimeMillis();
        csvUtility.updateTable(path, name, connection);
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        long minutes = executionTime / 60000;
        long seconds = (executionTime % 60000) / 1000;
        long milliseconds = executionTime % 1000;
        System.out.println("Table creation took " + minutes + " minutes, " + seconds + " seconds, and " + milliseconds + " milliseconds.");

        //Close resources
        statement.close();
        connection.close();

      
        
        

        //Exetcute a prepared query
        // String sql = "INSERT INTO table_name (column1, column2, column3) VALUES (?, ?, ?)";
        // PreparedStatement statement = connection.prepareStatement(sql);

        //String createTableQuery = "CREATE TABLE " + name + " (id INT PRIMARY KEY, name VARCHAR(255), age INT)";

    }
}
