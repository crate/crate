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

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.analyze.relations.FieldProvider;
import io.crate.data.Row;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.PartitionedBy;
import org.elasticsearch.cluster.metadata.IndexMetaData;

import java.util.Collections;
import java.util.Locale;

public final class CreateTableStatementAnalyzer {

    private static final String CLUSTERED_BY_IN_PARTITIONED_ERROR = "Cannot use CLUSTERED BY column in PARTITIONED BY clause";
    private final Schemas schemas;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;
    private final Functions functions;
    private final NumberOfShards numberOfShards;

    public CreateTableStatementAnalyzer(Schemas schemas,
                                        FulltextAnalyzerResolver fulltextAnalyzerResolver,
                                        Functions functions,
                                        NumberOfShards numberOfShards) {
        this.schemas = schemas;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
        this.functions = functions;
        this.numberOfShards = numberOfShards;
    }

    public AnalyzedCreateTable analyze(CreateTable<Expression> createTable,
                                       ParamTypeHints typeHints,
                                       CoordinatorTxnCtx txnCtx) {
        RelationName relationName = RelationName
            .of(createTable.name().getName(), txnCtx.sessionContext().searchPath().currentSchema());
        relationName.ensureValidForRelationCreation();

        var exprAnalyzerWithoutFields = new ExpressionAnalyzer(
            functions, txnCtx, typeHints, FieldProvider.UNSUPPORTED, null);
        var exprAnalyzerWithFieldsAsString = new ExpressionAnalyzer(
            functions, txnCtx, typeHints, FieldProvider.FIELDS_AS_STRING, null);
        var exprCtx = new ExpressionAnalysisContext();

        CreateTable<Symbol> analyzedCreateTable = new CreateTable<>(
            createTable.name().map(x -> exprAnalyzerWithFieldsAsString.convert(x, exprCtx)),
            createTable.tableElements(),
            createTable.partitionedBy().map(x -> x.map(y -> exprAnalyzerWithFieldsAsString.convert(y, exprCtx))),
            createTable.clusteredBy().map(x -> x.map(y -> exprAnalyzerWithFieldsAsString.convert(y, exprCtx))),
            createTable.properties().map(x -> exprAnalyzerWithoutFields.convert(x, exprCtx)),
            createTable.ifNotExists()
        );
        return new AnalyzedCreateTable(relationName, analyzedCreateTable);
    }

    public CreateTableAnalyzedStatement analyze(CreateTable<Expression> createTable,
                                                ParameterContext parameterContext,
                                                CoordinatorTxnCtx coordinatorTxnCtx) {
        CreateTableAnalyzedStatement statement = new CreateTableAnalyzedStatement();
        Row parameters = parameterContext.parameters();
        RelationName relationName = RelationName
            .of(createTable.name().getName(), coordinatorTxnCtx.sessionContext().searchPath().currentSchema());
        statement.table(relationName, createTable.ifNotExists(), schemas);

        // apply default in case it is not specified in the genericProperties,
        // if it is it will get overwritten afterwards.
        TablePropertiesAnalyzer.analyze(
            statement.tableParameter(),
            TableParameters.TABLE_CREATE_PARAMETER_INFO,
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
            coordinatorTxnCtx);

        // update table settings
        statement.tableParameter().settingsBuilder().put(tableElements.settings());
        statement.tableParameter().settingsBuilder().put(
            IndexMetaData.SETTING_NUMBER_OF_SHARDS, numberOfShards.defaultNumberOfShards());

        statement.analyzedTableElements(tableElements);
        createTable.clusteredBy().ifPresent(clusteredBy -> processClusteredBy(clusteredBy, statement, parameterContext));
        createTable.partitionedBy().ifPresent(partitionedBy -> processPartitionedBy(partitionedBy, statement, parameterContext));
        return statement;
    }

    private void processClusteredBy(ClusteredBy<Expression> clusteredBy,
                                    CreateTableAnalyzedStatement statement,
                                    ParameterContext parameterContext) {
        if (clusteredBy.column().isPresent()) {
            ColumnIdent routingColumn = ColumnIdent.fromPath(
                ExpressionToStringVisitor.convert(clusteredBy.column().get(), parameterContext.parameters()));

            for (AnalyzedColumnDefinition column : statement.analyzedTableElements().partitionedByColumns) {
                if (column.ident().equals(routingColumn)) {
                    throw new IllegalArgumentException(CLUSTERED_BY_IN_PARTITIONED_ERROR);
                }
            }
            if (!statement.hasColumnDefinition(routingColumn)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid or non-existent routing column \"%s\"",
                        routingColumn));
            }
            if (statement.primaryKeys().size() > 0 &&
                !statement.primaryKeys().contains(routingColumn.fqn())) {
                throw new IllegalArgumentException("Clustered by column must be part of primary keys");
            }

            statement.routing(routingColumn);
        }
        statement.tableParameter().settingsBuilder().put(
            IndexMetaData.SETTING_NUMBER_OF_SHARDS,
            numberOfShards.fromClusteredByClause(clusteredBy, parameterContext.parameters())
        );
    }

    private void processPartitionedBy(PartitionedBy<Expression> node,
                                      CreateTableAnalyzedStatement statement,
                                      ParameterContext parameterContext) {
        for (Expression partitionByColumn : node.columns()) {
            ColumnIdent partitionedByIdent = ColumnIdent.fromPath(
                ExpressionToStringVisitor.convert(partitionByColumn, parameterContext.parameters()));
            statement.analyzedTableElements().changeToPartitionedByColumn(partitionedByIdent, false, statement.tableIdent());
            ColumnIdent routing = statement.routing();
            if (routing != null && routing.equals(partitionedByIdent)) {
                throw new IllegalArgumentException(CLUSTERED_BY_IN_PARTITIONED_ERROR);
            }
        }
    }
}
