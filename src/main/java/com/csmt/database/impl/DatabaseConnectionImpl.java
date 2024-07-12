package com.csmt.database.impl;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.csmt.database.DatabaseConnection;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DatabaseConnectionImpl implements DatabaseConnection {

     private static HikariDataSource dataSource;

     static {
         HikariConfig config = new HikariConfig();

         // JDBC URL for SQL Server
         config.setJdbcUrl("jdbc:sqlserver://localhost:1433;databaseName=testinator;encrypt=false");
         config.setUsername("SA");
         config.setPassword("Vaibhav0921!");

         // Cache prepared statements for better performance
         config.addDataSourceProperty("cachePrepStmts", "true");
         config.addDataSourceProperty("prepStmtCacheSize", "250");
         config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

         // Set connection pool size
         config.setMaximumPoolSize(10); // Adjust the pool size based on your application's needs

         // Add timeout settings to avoid connection reset issues
         config.setConnectionTimeout(30000); // 30 seconds
         config.setIdleTimeout(600000); // 10 minutes
         config.setMaxLifetime(1800000); // 30 minutes

         // Additional properties to optimize performance and stability
         config.addDataSourceProperty("loginTimeout", "30");
         config.addDataSourceProperty("socketTimeout", "300000");

         // Initialize the data source
         dataSource = new HikariDataSource(config);
     }

    public static DataSource getDataSource() {
        return dataSource;
    }

    

    @Override
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
    
}
