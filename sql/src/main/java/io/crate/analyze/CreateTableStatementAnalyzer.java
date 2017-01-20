/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */
package io.crate.analyze;

import com.google.common.collect.ImmutableList;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.core.collections.Row;
import io.crate.metadata.*;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.pg_catalog.PgCatalogSchemaInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.sql.tree.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;

import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

public class CreateTableStatementAnalyzer extends DefaultTraversalVisitor<CreateTableAnalyzedStatement,
    CreateTableStatementAnalyzer.Context> {

    private static final TablePropertiesAnalyzer TABLE_PROPERTIES_ANALYZER = new TablePropertiesAnalyzer();
    private static final String CLUSTERED_BY_IN_PARTITIONED_ERROR = "Cannot use CLUSTERED BY column in PARTITIONED BY clause";
    private final Schemas schemas;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;
    private final Functions functions;
    private final NumberOfShards numberOfShards;

    static final Collection<String> READ_ONLY_SCHEMAS = ImmutableList.of(
        SysSchemaInfo.NAME,
        InformationSchemaInfo.NAME,
        PgCatalogSchemaInfo.NAME
    );

    static class Context {

        private final CreateTableAnalyzedStatement statement;
        private final ParameterContext parameterContext;

        public Context(CreateTableAnalyzedStatement statement, ParameterContext parameterContext) {
            this.statement = statement;
            this.parameterContext = parameterContext;
        }
    }


    public CreateTableStatementAnalyzer(Schemas schemas,
                                        FulltextAnalyzerResolver fulltextAnalyzerResolver,
                                        Functions functions,
                                        NumberOfShards numberOfShards) {
        this.schemas = schemas;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
        this.functions = functions;
        this.numberOfShards = numberOfShards;
    }

    @Override
    protected CreateTableAnalyzedStatement visitNode(Node node, Context context) {
        throw new RuntimeException(
            String.format(Locale.ENGLISH, "Encountered node %s but expected a CreateTable node", node));
    }

    public CreateTableAnalyzedStatement analyze(CreateTable createTable,
                                                ParameterContext parameterContext,
                                                SessionContext sessionContext) {
        CreateTableAnalyzedStatement statement = new CreateTableAnalyzedStatement();
        Row parameters = parameterContext.parameters();

        TableIdent tableIdent = getTableIdent(createTable, sessionContext);
        statement.table(tableIdent, createTable.ifNotExists(), schemas);

        // apply default in case it is not specified in the genericProperties,
        // if it is it will get overwritten afterwards.
        TABLE_PROPERTIES_ANALYZER.analyze(
            statement.tableParameter(),
            new TableParameterInfo(),
            createTable.properties(),
            parameters,
            true
        );
        AnalyzedTableElements tableElements = TableElementsAnalyzer.analyze(
            createTable.tableElements(), parameters, fulltextAnalyzerResolver, null);

        // validate table elements
        tableElements.finalizeAndValidate(
            tableIdent,
            Collections.emptyList(),
            functions,
            parameterContext,
            sessionContext);

        // update table settings
        statement.tableParameter().settingsBuilder().put(tableElements.settings());
        statement.tableParameter().settingsBuilder().put(
            IndexMetaData.SETTING_NUMBER_OF_SHARDS, numberOfShards.defaultNumberOfShards());

        Context context = new Context(statement, parameterContext);
        statement.analyzedTableElements(tableElements);
        for (CrateTableOption option : createTable.crateTableOptions()) {
            process(option, context);
        }
        return statement;
    }

    private TableIdent getTableIdent(CreateTable node, SessionContext sessionContext) {
        TableIdent tableIdent = TableIdent.of(node.name(), sessionContext.defaultSchema());
        if (READ_ONLY_SCHEMAS.contains(tableIdent.schema())) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Cannot create table in read-only schema '%s'", tableIdent.schema())
            );
        }
        return tableIdent;
    }

    @Override
    public CreateTableAnalyzedStatement visitClusteredBy(ClusteredBy clusteredBy, Context context) {
        if (clusteredBy.column().isPresent()) {
            ColumnIdent routingColumn = ColumnIdent.fromPath(
                ExpressionToStringVisitor.convert(clusteredBy.column().get(), context.parameterContext.parameters()));

            for (AnalyzedColumnDefinition column : context.statement.analyzedTableElements().partitionedByColumns) {
                if (column.ident().equals(routingColumn)) {
                    throw new IllegalArgumentException(CLUSTERED_BY_IN_PARTITIONED_ERROR);
                }
            }
            if (!context.statement.hasColumnDefinition(routingColumn)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid or non-existent routing column \"%s\"",
                        routingColumn));
            }
            if (context.statement.primaryKeys().size() > 0 &&
                !context.statement.primaryKeys().contains(routingColumn.fqn())) {
                throw new IllegalArgumentException("Clustered by column must be part of primary keys");
            }

            context.statement.routing(routingColumn);
        }
        context.statement.tableParameter().settingsBuilder().put(
            IndexMetaData.SETTING_NUMBER_OF_SHARDS,
            numberOfShards.fromClusteredByClause(clusteredBy, context.parameterContext.parameters())
        );
        return context.statement;
    }

    @Override
    public CreateTableAnalyzedStatement visitPartitionedBy(PartitionedBy node, Context context) {
        for (Expression partitionByColumn : node.columns()) {
            ColumnIdent partitionedByIdent = ColumnIdent.fromPath(
                ExpressionToStringVisitor.convert(partitionByColumn, context.parameterContext.parameters()));
            context.statement.analyzedTableElements().changeToPartitionedByColumn(partitionedByIdent, false);
            ColumnIdent routing = context.statement.routing();
            if (routing != null && routing.equals(partitionedByIdent)) {
                throw new IllegalArgumentException(CLUSTERED_BY_IN_PARTITIONED_ERROR);
            }
        }
        return null;
    }
}
