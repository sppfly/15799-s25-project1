package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;

public class App {

    private static final String OUTPUT_PATH = "/workspaces/15799-s25-project1/output";

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

        // Note: in practice, you would probably use org.apache.calcite.tools.Frameworks.
        // That package provides simple defaults that make it easier to configure Calcite.
        // But there's a lot of magic happening there; since this is an educational project,
        // we guide you towards the explicit method in the writeup.
        // var conn = JDBCUtil.connect();
        // var schema = JDBCUtil.getSchema(conn);
        SchemaPlus rootSchema = JDBCUtil.getRootSchema();
        Query query = new Query(rootSchema);

        // foo.sql: the original SQL query
        // foo.txt: the initial RelNode plan of the original SQL query, Logical, before any optimizations
        // foo_optimized.txt: the final optimized RelNode plan, Enumerable, after all your optimizations
        // foo_results.csv: the results of executing your optimized plan in Calcite
        // foo_optimized.sql: your optimized plan deparsed into a SQL query
        var sqls = query.getSqls(arg1);
        for (var sql : sqls.entrySet()) {
            try {
                RelRoot relRoot = query.parseSql(sql.getValue());
                Path path = Path.of(OUTPUT_PATH, sql.getKey());

                File dir = path.toFile();
                if (dir.exists()) {
                    dir.delete();
                }
                dir.mkdir();

                var original = Path.of(path.toString(), String.format("%s.sql", sql.getKey()));
                Files.writeString(original, sql.getValue());

                var originalPlan = Path.of(path.toString(), String.format("%s.txt", sql.getKey()));
                Util.SerializePlan(relRoot.rel, originalPlan.toFile());

                break;
            } catch (IOException | SqlParseException e) {
                System.out.println(String.format("get exception on %s", sql.getKey()));
                throw e;
            }
        }
    }
}
