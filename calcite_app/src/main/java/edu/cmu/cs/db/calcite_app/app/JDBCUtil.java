package edu.cmu.cs.db.calcite_app.app;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.dbcp2.BasicDataSource;

public class JDBCUtil {

    public static final String URL = "jdbc:duckdb:/workspaces/15799-s25-project1/test.db";

    public static Connection connect() throws SQLException, ClassNotFoundException {
        Properties info = new Properties();
        info.put("model", "/workspaces/15799-s25-project1/calcite_app/src/main/resources/model.json");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        return DriverManager.getConnection(URL, info);
    }

    public static JdbcSchema getSchema(Connection connection) throws SQLException {
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        
        var dataSource = new BasicDataSource();
        dataSource.setUrl(URL);

        JdbcSchema schema = JdbcSchema.create(rootSchema, null, dataSource, null, null);
        return schema;
    }



    public static void test() throws SQLException, ClassNotFoundException {

        // Create Calcite root schema
        Properties props = new Properties();
        Connection connection = DriverManager.getConnection("jdbc:calcite:", props);
        CalciteConnection calciteConn = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConn.getRootSchema();

        // Connect to DuckDB via JDBC
        Class.forName("org.duckdb.DuckDBDriver");

        var dataSource = new BasicDataSource();
        dataSource.setUrl(URL);

        // Create JdbcSchema manually
        JdbcSchema duckSchema = JdbcSchema.create(
            rootSchema,           // Parent schema
            "DUCK",               // Schema name
            dataSource,           // JDBC connection to DuckDB
            null,                 // Catalog (optional)
            null                  // Schema pattern (optional)
        );
        // Add the schema to the root
        rootSchema.add("DUCK", duckSchema);
        System.out.println(duckSchema.getTableNames());
    }
}

