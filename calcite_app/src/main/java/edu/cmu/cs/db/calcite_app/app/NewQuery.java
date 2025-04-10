package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor.Result;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

public class NewQuery {

    static String OUTPUT_PATH = "/workspaces/15799-s25-project1/output";

    private VolcanoPlanner volcanoPlanner;

    private RelOptCluster sharedCluster;

    private SqlParser.Config parserConfig;

    private SqlValidator validator;

    private SqlToRelConverter converter;

    public static NewQuery init(SchemaPlus rootSchema) {

        VolcanoPlanner volcanoPlanner = new VolcanoPlanner();
        volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelOptCluster sharedCluster = RelOptCluster.create(volcanoPlanner, new RexBuilder(typeFactory));

        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                List.of("DUCK"), // default path
                typeFactory,
                new CalciteConnectionConfigImpl(properties)
        );

        SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory,
                SqlValidator.Config.DEFAULT
        );
        SqlToRelConverter converter = new SqlToRelConverter(
                null, // ViewExpander (optional)
                validator,
                catalogReader,
                sharedCluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config()
        );

        NewQuery newQuery = new NewQuery();
        newQuery.sharedCluster = sharedCluster;
        newQuery.volcanoPlanner = volcanoPlanner;
        newQuery.validator = validator;
        newQuery.converter = converter;
        newQuery.parserConfig = SqlParser.Config.DEFAULT;
        return newQuery;
    }

    public RelNode parseAndValidate(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, parserConfig);
        SqlNode parsed = parser.parseQuery();
        SqlNode validated = validator.validate(parsed);
        RelRoot relRoot = converter.convertQuery(validated, false, true);
        return relRoot.rel;
    }

    public RelNode hep(RelNode logicalPlan) {
        HepProgram hepProgram = new HepProgramBuilder()
                .addRuleInstance(CoreRules.JOIN_CONDITION_PUSH)
                .addRuleInstance(CoreRules.JOIN_PUSH_EXPRESSIONS)
                .addRuleInstance(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES)
                .addRuleInstance(CoreRules.PROJECT_CORRELATE_TRANSPOSE)
                .addRuleInstance(CoreRules.FILTER_CORRELATE)
                .addRuleInstance(CoreRules.PROJECT_MERGE)
                .addRuleInstance(CoreRules.FILTER_MERGE)
                .addRuleInstance(CoreRules.JOIN_CONDITION_PUSH)
                .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
                .addRuleInstance(CoreRules.JOIN_CONDITION_PUSH)
                .addRuleInstance(CoreRules.JOIN_COMMUTE)
                .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
                .build();

        HepPlanner hepPlanner = new HepPlanner(hepProgram);
        hepPlanner.setRoot(logicalPlan);
        return hepPlanner.findBestExp();
    }

    public RelNode volcano(RelNode simplified) {

        // Set the trait to target EnumerableConvention
        RelTraitSet traitAdjusted = sharedCluster.traitSet()
                .replace(EnumerableConvention.INSTANCE)
                .simplify();

        RelNode rootWithTrait = volcanoPlanner.changeTraits(simplified, traitAdjusted);
        volcanoPlanner.setRoot(rootWithTrait);

        // Get final optimized executable plan
        return volcanoPlanner.findBestExp();

    }

    /**
     * return the list of SQL queries under a dir
     *
     * @param path the path of the dir
     * @return
     * @throws java.io.IOException
     */
    public Map<String, String> getSqls(String path) throws IOException {
        File dir = new File(path);
        File[] sqlFiles = dir.listFiles((d, file) -> file.endsWith(".sql"));
        var res = new HashMap<String, String>();
        for (var file : sqlFiles) {
            res.put(file.getName(), Files.readString(file.toPath()));
        }
        return res;
    }

    public static String deParseSqlToDuckDB(RelNode relNode) {
        var converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
        Result result = converter.visitRoot(relNode);
        SqlNode sqlNode = result.asStatement();
        return sqlNode.toSqlString(PostgresqlSqlDialect.DEFAULT).getSql();
    }


    /*** Helper functions to write the result out */


    public void writeOriginalSql(String name, String sql) throws IOException {
        var path = Path.of(OUTPUT_PATH, name, String.format("%s.sql", name));
        Files.writeString(path, sql);
    }

    public void writeOriginalPlan(String name, RelNode relNode) throws IOException {
        var path = Path.of(OUTPUT_PATH, name, String.format("%s.txt", name));
        Util.SerializePlan(relNode, path.toFile());
    }

    public void writeOptimizedPlan(String name, RelNode relNode) throws IOException {
        var path = Path.of(OUTPUT_PATH, name, String.format("%s_optimized.txt", name));
        Util.SerializePlan(relNode, path.toFile());
    }

    public void writeOptimizedSql(String name, String sql) throws IOException {
        var path = Path.of(OUTPUT_PATH, name, String.format("%s_optimized.sql", name));
        Files.writeString(path, sql);
    }

    public void writeResult(String name, ResultSet resultSet) throws SQLException, IOException {
        var path = Path.of(OUTPUT_PATH, name, String.format("%s_results.csv", name));
        Util.serializeresultset(resultSet, path.toFile());
    }

    /**
     *
     * foo.sql: the original SQL query foo.txt: the initial RelNode plan of the
     * original SQL query, Logical, before any optimizations foo_optimized.txt:
     * the final optimized RelNode plan, Enumerable, after all your
     * optimizations foo_results.csv: the results of executing your optimized
     * plan in Calcite foo_optimized.sql: your optimized plan deparsed into a
     * SQL query
     *
     *
     * @param name
     * @param originalSql
     * @param optimizedSql
     * @param originalPlan
     * @param optimizedPlan
     * @param resultSet
     * @throws java.io.IOException
     * @throws java.sql.SQLException
     */
    public void writeOut(String name,
            String originalSql,
            RelNode originalPlan,
            String optimizedSql,
            RelNode optimizedPlan,
            ResultSet resultSet) throws IOException, SQLException {
        Path path = Path.of(OUTPUT_PATH, name);
        File dir = path.toFile();
        if (dir.exists()) {
            dir.delete();
        }
        dir.mkdir();

        writeOriginalSql(name, originalSql);
        writeOriginalPlan(name, originalPlan);
        writeOptimizedSql(name, optimizedSql);
        writeOptimizedPlan(name, optimizedPlan);
        writeResult(name, resultSet);
    }
}
