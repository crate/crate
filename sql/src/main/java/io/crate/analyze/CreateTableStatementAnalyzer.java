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
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.metadata.*;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.pg_catalog.PgCatalogSchemaInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.sql.tree.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Collection;
import java.util.Locale;

@Singleton
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
        Analysis analysis;
        CreateTableAnalyzedStatement statement;

        public Context(Analysis analysis) {
            this.analysis = analysis;
        }
    }

    public CreateTableAnalyzedStatement analyze(Node node, Analysis analysis) {
        return super.process(node, new Context(analysis));
    }

    @Inject
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

    @Override
    public CreateTableAnalyzedStatement visitCreateTable(CreateTable node, Context context) {
        assert context.statement == null;
        context.statement = new CreateTableAnalyzedStatement(fulltextAnalyzerResolver);
        setTableIdent(node, context);

        // apply default in case it is not specified in the genericProperties,
        // if it is it will get overwritten afterwards.
        TABLE_PROPERTIES_ANALYZER.analyze(
            context.statement.tableParameter(), new TableParameterInfo(),
            node.properties(), context.analysis.parameterContext().parameters(), true);

        context.statement.analyzedTableElements(TableElementsAnalyzer.analyze(
            node.tableElements(),
            context.analysis.parameterContext(),
            context.statement.fulltextAnalyzerResolver()));

        // validate table elements
        context.statement.analyzedTableElements().finalizeAndValidate(
            context.statement.tableIdent(),
            null,
            functions,
            context.analysis.parameterContext(),
            context.analysis.sessionContext());

        // update table settings
        context.statement.tableParameter().settingsBuilder().put(context.statement.analyzedTableElements().settings());
        context.statement.tableParameter().settingsBuilder().put(
            IndexMetaData.SETTING_NUMBER_OF_SHARDS,
            numberOfShards.defaultNumberOfShards()
        );

        for (CrateTableOption option : node.crateTableOptions()) {
            process(option, context);
        }

        return context.statement;
    }

    private void setTableIdent(CreateTable node, Context context) {
        TableIdent tableIdent = TableIdent.of(node.name(), context.analysis.sessionContext().defaultSchema());
        if (READ_ONLY_SCHEMAS.contains(tableIdent.schema())) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Cannot create table in read-only schema '%s'", tableIdent.schema())
            );
        }
        context.statement.table(tableIdent, node.ifNotExists(), schemas);
    }

    @Override
    public CreateTableAnalyzedStatement visitClusteredBy(ClusteredBy clusteredBy, Context context) {
        if (clusteredBy.column().isPresent()) {
            ColumnIdent routingColumn = ColumnIdent.fromPath(
                ExpressionToStringVisitor.convert(clusteredBy.column().get(), context.analysis.parameterContext().parameters()));

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
            numberOfShards.fromClusteredByClause(clusteredBy, context.analysis.parameterContext().parameters())
        );
        return context.statement;
    }

    @Override
    public CreateTableAnalyzedStatement visitPartitionedBy(PartitionedBy node, Context context) {
        for (Expression partitionByColumn : node.columns()) {
            ColumnIdent partitionedByIdent = ColumnIdent.fromPath(
                ExpressionToStringVisitor.convert(partitionByColumn, context.analysis.parameterContext().parameters()));
            context.statement.analyzedTableElements().changeToPartitionedByColumn(partitionedByIdent, false);
            ColumnIdent routing = context.statement.routing();
            if (routing != null && routing.equals(partitionedByIdent)) {
                throw new IllegalArgumentException(CLUSTERED_BY_IN_PARTITIONED_ERROR);
            }
        }
        return null;
    }
}
