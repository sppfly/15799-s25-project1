package edu.cmu.cs.db.calcite_app.app;

import java.io.IOException;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;

public class App {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: java -jar App.jar <arg1> <arg2>");
            return;
        }

        // Feel free to modify this to take as many or as few arguments as you want.
        System.out.println("Running the app!");
        String arg1 = args[0];
        System.out.println("\tArg1: " + arg1);
        String arg2 = args[1];
        System.out.println("\tArg2: " + arg2);

        SchemaPlus rootSchema = JDBCUtil.getRootSchema();
        NewQuery newQuery = NewQuery.init(rootSchema);

        var sqls = newQuery.getSqls(arg1);
        for (var sql : sqls.entrySet()) {
            try {
                if (!sql.getKey().equals("simple.sql")) {
                    continue;
                }
                var relNode = newQuery.parseAndValidate(sql.getValue());
                var rewroteQuery = newQuery.hep(relNode);
                var optimized = newQuery.volcano(rewroteQuery);
                var name = sql.getKey().split("\\.")[0];

                newQuery.writeOut(name,
                        sql.getValue(),
                        relNode,
                        // NewQuery.deParseSqlToDuckDB(optimized),
                        null,
                        optimized,
                        null);

            } catch (IOException | SqlParseException e) {
                throw e;
            }
        }
    }
}
