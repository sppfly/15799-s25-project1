package edu.cmu.cs.db.calcite_app.app;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.duckdb.DuckDBConnection;

public class JDBCUtil {

    public static final String URL = "jdbc:duckdb:/workspaces/15799-s25-project1/test.db";

    public static DuckDBConnection connect() throws SQLException {
        Properties readOnlyProperty = new Properties();
        readOnlyProperty.setProperty("duckdb.read_only", "true");
        // what does this do?
        readOnlyProperty.setProperty("lex", "JAVA");

        return (DuckDBConnection) DriverManager.getConnection(URL, readOnlyProperty);
    }

    public static JdbcSchema getSchema(DuckDBConnection connection) throws SQLException {
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        DataSource dataSource = new DataSource();
        dataSource.setUrl(URL);

        JdbcSchema schema = JdbcSchema.create(rootSchema, null, dataSource, null, null);
        return schema;
    }

}