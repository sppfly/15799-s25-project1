package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
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

import jdk.jfr.Percentage;

public class Query {

    private final SchemaPlus rootSchema;

    private SqlValidator sqlValidator;

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
                new CalciteConnectionConfigImpl(properties)
        );
        SqlOperatorTable operatorTable = SqlStdOperatorTable.instance();
        var validator = SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory, SqlValidator.Config.DEFAULT);

        // Create a cluster with a planner and rex builder
        RelOptCluster cluster = RelOptCluster.create(
                new VolcanoPlanner(),
                new RexBuilder(typeFactory)
        );

        var converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(true);

        SqlToRelConverter converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig
        );

        this.sqlValidator = validator;
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
        var parser = SqlParser.create(sqlString);

        System.out.println(SqlParser.config().caseSensitive());
        SqlNode node = parser.parseQuery();

        System.out.println(node);

        // SqlNode validatedNode = sqlValidator.validate(node);

        RelRoot relRoot = this.sqlToRelConverter.convertQuery(node, true, true);
        return relRoot;
    }

    public static String deParseSqlToDuckDB(SqlNode sqlNode) {
        return sqlNode.toSqlString(PostgresqlSqlDialect.DEFAULT).getSql();
    }
}
