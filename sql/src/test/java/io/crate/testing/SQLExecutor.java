/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.testing;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.Analysis;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.SubqueryAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.ParentRelations;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.analyze.relations.StatementAnalysisContext;
import io.crate.analyze.repositories.RepositoryParamValidator;
import io.crate.analyze.repositories.RepositorySettingsModule;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.data.Rows;
import io.crate.executor.transport.RepositoryService;
import io.crate.metadata.Functions;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocSchemaInfoFactory;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.TestingDocTableInfoFactory;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.udf.UserDefinedFunctionService;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.TableStats;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.QualifiedName;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static io.crate.analyze.TableDefinitions.DEEPLY_NESTED_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.NESTED_PK_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.TEST_CLUSTER_BY_STRING_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.TEST_DOC_LOCATIONS_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.TEST_DOC_TRANSACTIONS_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.TEST_MULTIPLE_PARTITIONED_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.TEST_NESTED_PARTITIONED_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.TEST_PARTITIONED_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.USER_TABLE_INFO;
import static io.crate.analyze.TableDefinitions.USER_TABLE_INFO_CLUSTERED_BY_ONLY;
import static io.crate.analyze.TableDefinitions.USER_TABLE_INFO_MULTI_PK;
import static io.crate.analyze.TableDefinitions.USER_TABLE_INFO_REFRESH_INTERVAL_BY_ONLY;
import static io.crate.testing.TestingHelpers.getFunctions;
import static org.mockito.Mockito.mock;

/**
 * Lightweight alternative to {@link SQLTransportExecutor}.
 *
 * Can be used for unit-tests testss which don't require the full execution-layer/nodes to be started.
 */
public class SQLExecutor {

    private static final Logger LOGGER = Loggers.getLogger(SQLExecutor.class);

    private final Functions functions;
    public final Analyzer analyzer;
    public final Planner planner;
    private final RelationAnalyzer relAnalyzer;
    private final SessionContext sessionContext;

    public static class Builder {

        private final ClusterService clusterService;
        private final Map<String, SchemaInfo> schemaInfoByName = new HashMap<>();
        private final Map<TableIdent, DocTableInfo> docTables = new HashMap<>();
        private final Map<TableIdent, BlobTableInfo> blobTables = new HashMap<>();
        private final Functions functions;
        private String defaultSchema = Schemas.DOC_SCHEMA_NAME;

        private TableStats tableStats = new TableStats();

        public Builder(ClusterService clusterService) {
            this.clusterService = clusterService;
            schemaInfoByName.put("sys", new SysSchemaInfo(clusterService));
            schemaInfoByName.put("information_schema", new InformationSchemaInfo(clusterService));
            functions = getFunctions();
        }

        public Builder setDefaultSchema(String schema) {
            this.defaultSchema = schema;
            return this;
        }

        /**
         * Adds a couple of tables which are defined in {@link T3} and {@link io.crate.analyze.TableDefinitions}.
         * <p>
         * Note that these tables do have a static routing which doesn't necessarily match the nodes that
         * are part of the clusterState of the {@code clusterService} provided to the builder.
         * </p>
         */
        public Builder enableDefaultTables() {
            // we should try to reduce the number of tables here eventually...
            addDocTable(USER_TABLE_INFO);
            addDocTable(USER_TABLE_INFO_CLUSTERED_BY_ONLY);
            addDocTable(USER_TABLE_INFO_MULTI_PK);
            addDocTable(DEEPLY_NESTED_TABLE_INFO);
            addDocTable(NESTED_PK_TABLE_INFO);
            addDocTable(TEST_PARTITIONED_TABLE_INFO);
            addDocTable(TEST_NESTED_PARTITIONED_TABLE_INFO);
            addDocTable(TEST_MULTIPLE_PARTITIONED_TABLE_INFO);
            addDocTable(TEST_DOC_TRANSACTIONS_TABLE_INFO);
            addDocTable(TEST_DOC_LOCATIONS_TABLE_INFO);
            addDocTable(TEST_CLUSTER_BY_STRING_TABLE_INFO);
            addDocTable(USER_TABLE_INFO_REFRESH_INTERVAL_BY_ONLY);
            addDocTable(T3.T1_INFO);
            addDocTable(T3.T2_INFO);
            addDocTable(T3.T3_INFO);
            return this;
        }

        public SQLExecutor build() {
            UserDefinedFunctionService udfService = new UserDefinedFunctionService(clusterService, functions);
            schemaInfoByName.put(
                defaultSchema,
                new DocSchemaInfo(defaultSchema, clusterService, functions, udfService,
                    new TestingDocTableInfoFactory(docTables)));
            if (!blobTables.isEmpty()) {
                schemaInfoByName.put(
                    BlobSchemaInfo.NAME,
                    new BlobSchemaInfo(clusterService, new TestingBlobTableInfoFactory(blobTables)));
            }
            File tempDir = createTempDir();
            Schemas schemas = new Schemas(
                Settings.EMPTY,
                schemaInfoByName,
                clusterService,
                new DocSchemaInfoFactory(new TestingDocTableInfoFactory(Collections.emptyMap()), functions, udfService)
            );
            return new SQLExecutor(
                functions,
                new Analyzer(
                    schemas,
                    functions,
                    clusterService,
                    new AnalysisRegistry(
                        new Environment(Settings.builder()
                            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir.toString())
                            .build()),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap()
                    ),
                    new RepositoryService(
                        clusterService,
                        mock(TransportDeleteRepositoryAction.class),
                        mock(TransportPutRepositoryAction.class)
                    ),
                    new ModulesBuilder().add(new RepositorySettingsModule())
                        .createInjector()
                        .getInstance(RepositoryParamValidator.class)
                ),
                new Planner(
                    clusterService,
                    functions,
                    tableStats
                ),
                new RelationAnalyzer(clusterService, functions, schemas),
                new SessionContext(defaultSchema, null, s -> {}, t -> {})
            );
        }

        private static File createTempDir() {
            int attempt = 0;
            while (attempt < 3) {
                try {
                    attempt++;
                    File tempDir = File.createTempFile("temp", Long.toString(System.nanoTime()));
                    tempDir.deleteOnExit();
                    return tempDir;
                } catch (IOException e) {
                    LOGGER.warn("Unable to create temp dir on attempt {} due to {}", attempt, e.getMessage());
                }
            }
            throw new IllegalStateException("Cannot create temp dir");
        }

        public Builder addSchema(SchemaInfo schema) {
            schemaInfoByName.put(schema.name(), schema);
            return this;
        }

        public Builder addDocTable(DocTableInfo table) {
            docTables.put(table.ident(), table);
            return this;
        }

        public Builder addDocTable(TestingTableInfo.Builder builder) {
            return addDocTable(builder.build(functions));
        }

        public Builder addBlobTable(BlobTableInfo tableInfo) {
            blobTables.put(tableInfo.ident(), tableInfo);
            return this;
        }

        public Builder setTableStats(TableStats tableStats) {
            this.tableStats = tableStats;
            return this;
        }
    }

    public static Builder builder(ClusterService clusterService) {
        return new Builder(clusterService);
    }

    private SQLExecutor(Functions functions, Analyzer analyzer, Planner planner, RelationAnalyzer relAnalyzer, SessionContext sessionContext) {
        this.functions = functions;
        this.analyzer = analyzer;
        this.planner = planner;
        this.relAnalyzer = relAnalyzer;
        this.sessionContext = sessionContext;
    }

    public Functions functions() {
        return functions;
    }

    private <T extends AnalyzedStatement> T analyze(String stmt, ParameterContext parameterContext) {
        Analysis analysis = analyzer.boundAnalyze(
            SqlParser.createStatement(stmt), sessionContext, parameterContext);
        //noinspection unchecked
        return (T) analysis.analyzedStatement();
    }

    public <T extends AnalyzedStatement> T analyze(String statement) {
        return analyze(statement, ParameterContext.EMPTY);
    }

    public <T extends AnalyzedStatement> T analyze(String statement, Object[] arguments) {
        return analyze(
            statement,
            arguments.length == 0
                ? ParameterContext.EMPTY
                : new ParameterContext(new RowN(arguments), Collections.emptyList()));
    }

    public <T extends AnalyzedStatement> T analyze(String statement, Object[][] bulkArgs) {
        return analyze(statement, new ParameterContext(Row.EMPTY, Rows.of(bulkArgs)));
    }

    /**
     * Convert a expression to a symbol
     *
     * @param sources The relations which are accessible in the expression.
     *                Think of this as the FROM clause.
     *                If tables are used here they must also be registered in the SQLExecutor having used {@link Builder#addDocTable(DocTableInfo)}
     */
    public Symbol asSymbol(Map<QualifiedName, AnalyzedRelation> sources, String expression) {
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            sessionContext,
            ParamTypeHints.EMPTY,
            new FullQualifiedNameFieldProvider(sources, ParentRelations.NO_PARENTS, sessionContext.defaultSchema()),
            new SubqueryAnalyzer(
                relAnalyzer,
                new StatementAnalysisContext(sessionContext, ParamTypeHints.EMPTY, Operation.READ, new TransactionContext(sessionContext))
            )
        );
        return expressionAnalyzer.convert(SqlParser.createExpression(expression), new ExpressionAnalysisContext());
    }


    public <T extends Plan> T plan(String statement, UUID jobId, int softLimit, int fetchSize) {
        Analysis analysis = analyzer.boundAnalyze(
            SqlParser.createStatement(statement), sessionContext, ParameterContext.EMPTY);
        //noinspection unchecked
        return (T) planner.plan(analysis, jobId, softLimit, fetchSize);
    }

    public <T extends Plan> T plan(String stmt, Object[][] bulkArgs) {
        Analysis analysis = analyzer.boundAnalyze(
            SqlParser.createStatement(stmt),
            sessionContext,
            new ParameterContext(Row.EMPTY, Rows.of(bulkArgs)));
        //noinspection unchecked
        return (T) planner.plan(analysis, UUID.randomUUID(), 0, 0);
    }

    public <T extends Plan> T plan(String statement) {
        return plan(statement, UUID.randomUUID(), 0, 0);
    }
}
