package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
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
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

public class Query {

    private final SchemaPlus rootSchema;

    private SqlToRelConverter sqlToRelConverter;

    public Query(SchemaPlus rootSchema) {
        this.rootSchema = rootSchema;
        init();
    }

    private void init() {

        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema.getSubSchema("DUCK")),
                List.of(), // default schema path
                typeFactory,
                new CalciteConnectionConfigImpl(properties));
        SqlOperatorTable operatorTable = SqlStdOperatorTable.instance();
        var validator = SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory,
                SqlValidator.Config.DEFAULT);

        // Create a cluster with a planner and rex builder
        RelOptCluster cluster = RelOptCluster.create(
                new VolcanoPlanner(),
                new RexBuilder(typeFactory));

        var converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(true);

        SqlToRelConverter converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig);

        this.sqlToRelConverter = converter;
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

    public RelRoot parseSql(String sqlString) throws SqlParseException {
        SqlParser parser = SqlParser.create(sqlString);
        SqlNode node = parser.parseQuery();
        RelRoot relRoot = this.sqlToRelConverter.convertQuery(node, true, true);
        return relRoot;
    }

    public RelNode optimze(RelNode relNode) {
        var node = heuristic(relNode);
        // return volcano(node);
        return node;
    }

    public RelNode heuristic(RelNode relNode) {
        // Step 1: Use HepPlanner for pre-processing
        HepProgram hepProgram = new HepProgramBuilder()
                .addRuleInstance(CoreRules.FILTER_INTO_JOIN.config.toRule())
                .build();

        HepPlanner hepPlanner = new HepPlanner(hepProgram);
        hepPlanner.setRoot(relNode);

        return hepPlanner.findBestExp();
    }

    public RelNode volcano(RelNode relNode) {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        // planner.addRule(CoreRules.FILlter);
        // planner.addRule(CoreRules.PROJECT_MERGE_RULE);
        planner.setRoot(relNode);
        return planner.findBestExp();
    }

    public static String deParseSqlToDuckDB(RelNode relNode) {
        var converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
        Result result = converter.visitRoot(relNode);
        SqlNode sqlNode = result.asStatement();
        return sqlNode.toSqlString(PostgresqlSqlDialect.DEFAULT).getSql();
    }
}
