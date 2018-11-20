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

import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.data.Row;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.CrateTableOption;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.PartitionedBy;
import org.elasticsearch.cluster.metadata.IndexMetaData;

import java.util.Collections;
import java.util.Locale;

public class CreateTableStatementAnalyzer extends DefaultTraversalVisitor<CreateTableAnalyzedStatement,
    CreateTableStatementAnalyzer.Context> {

    private static final String CLUSTERED_BY_IN_PARTITIONED_ERROR = "Cannot use CLUSTERED BY column in PARTITIONED BY clause";
    private final Schemas schemas;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;
    private final Functions functions;
    private final NumberOfShards numberOfShards;

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
                                                TransactionContext transactionContext) {
        CreateTableAnalyzedStatement statement = new CreateTableAnalyzedStatement();
        Row parameters = parameterContext.parameters();
        RelationName relationName = RelationName
            .of(createTable.name().getName(), transactionContext.sessionContext().searchPath().currentSchema());
        statement.table(relationName, createTable.ifNotExists(), schemas);

        // apply default in case it is not specified in the genericProperties,
        // if it is it will get overwritten afterwards.
        TablePropertiesAnalyzer.analyze(
            statement.tableParameter(),
            TableParameterInfo.TABLE_CREATE_PARAMETER_INFO,
            createTable.properties(),
            parameters,
            true
        );
        AnalyzedTableElements tableElements = TableElementsAnalyzer.analyze(
            createTable.tableElements(), parameters, fulltextAnalyzerResolver, relationName, null);

        // validate table elements
        tableElements.finalizeAndValidate(
            relationName,
            Collections.emptyList(),
            functions,
            parameterContext,
            transactionContext);

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
            context.statement.analyzedTableElements().changeToPartitionedByColumn(partitionedByIdent, false, context.statement.tableIdent());
            ColumnIdent routing = context.statement.routing();
            if (routing != null && routing.equals(partitionedByIdent)) {
                throw new IllegalArgumentException(CLUSTERED_BY_IN_PARTITIONED_ERROR);
            }
        }
        return null;
    }
}
