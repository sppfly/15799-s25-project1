package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
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
                if (sql.getKey().equals("q1.sql")) {
                    continue;
                }
                RelRoot relRoot = query.parseSql(sql.getValue());

                var name = sql.getKey().split("\\.")[0];

                Path path = Path.of(OUTPUT_PATH, name);

                File dir = path.toFile();
                if (dir.exists()) {
                    dir.delete();
                }
                dir.mkdir();

                var originalSqlPath = Path.of(path.toString(), String.format("%s.sql", name));
                Files.writeString(originalSqlPath, sql.getValue());

                var originalPlanPath = Path.of(path.toString(), String.format("%s.txt", name));
                Util.SerializePlan(relRoot.rel, originalPlanPath.toFile());

                var optimizedPlan = query.optimze(relRoot.rel);

                var optimizedPlanPath = Path.of(path.toString(), String.format("%s_optimzed.txt", name));
                Util.SerializePlan(optimizedPlan, optimizedPlanPath.toFile());

                var optimizedSql = Query.deParseSqlToDuckDB(optimizedPlan);
                var optimizedSqlPath = Path.of(path.toString(), String.format("%s_optimzed.sql", name));
                Files.writeString(optimizedSqlPath, optimizedSql);
                // break;
            } catch ( // IOException | 
                    SqlParseException e) {
                System.out.println(String.format("get exception on %s", sql.getKey()));
                throw e;
            }
        }
    }
}
