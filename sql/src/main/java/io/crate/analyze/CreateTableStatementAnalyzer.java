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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.crate.Constants;
import io.crate.analyze.expressions.ExpressionToNumberVisitor;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.sql.tree.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;

import java.util.Locale;


public class CreateTableStatementAnalyzer extends AbstractStatementAnalyzer<Void, CreateTableAnalyzedStatement> {

    private static final TablePropertiesAnalyzer TABLE_PROPERTIES_ANALYZER = new TablePropertiesAnalyzer();
    private final ReferenceInfos referenceInfos;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;

    @Inject
    public CreateTableStatementAnalyzer(ReferenceInfos referenceInfos,
                                        FulltextAnalyzerResolver fulltextAnalyzerResolver) {
        this.referenceInfos = referenceInfos;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
    }

    @Override
    public AnalyzedStatement newAnalysis(ParameterContext parameterContext) {
        return new CreateTableAnalyzedStatement(referenceInfos, fulltextAnalyzerResolver, parameterContext);
    }

    protected Void visitNode(Node node, CreateTableAnalyzedStatement context) {
        throw new RuntimeException(
                String.format("Encountered node %s but expected a CreateTable node", node));
    }

    @Override
    public Void visitCreateTable(CreateTable node, CreateTableAnalyzedStatement context) {
        TableIdent tableIdent = TableIdent.of(node.name());
        Preconditions.checkArgument(Strings.isNullOrEmpty(tableIdent.schema()),
                "A custom schema name must not be specified in the CREATE TABLE clause");
        context.table(tableIdent);

        // apply default in case it is not specified in the genericProperties,
        // if it is it will get overwritten afterwards.
        TABLE_PROPERTIES_ANALYZER.analyze(
                context.tableParameter(), new TableParameterInfo(),
                node.properties(), context.parameters(), true);

        context.analyzedTableElements(TableElementsAnalyzer.analyze(
                node.tableElements(),
                context.parameters(),
                context.fulltextAnalyzerResolver()));

        context.analyzedTableElements().finalizeAndValidate();
        // update table settings
        context.tableParameter().settingsBuilder().put(context.analyzedTableElements().settings());

        for (CrateTableOption option : node.crateTableOptions()) {
            process(option, context);
        }

        return null;
    }

    @Override
    public Void visitClusteredBy(ClusteredBy node, CreateTableAnalyzedStatement context) {
        if (node.column().isPresent()) {
            ColumnIdent routingColumn = ColumnIdent.fromPath(
                    ExpressionToStringVisitor.convert(node.column().get(), context.parameters()));

            if (!context.hasColumnDefinition(routingColumn)) {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "Invalid or non-existent routing column \"%s\"",
                                routingColumn));
            }
            if (context.primaryKeys().size() > 0 && !context.primaryKeys().contains(routingColumn.fqn())) {
                throw new IllegalArgumentException("Clustered by column must be part of primary keys");
            }

            context.routing(routingColumn);
        }

        int numShards;
        if (node.numberOfShards().isPresent()) {
            numShards = ExpressionToNumberVisitor.convert(node.numberOfShards().get(), context.parameters()).intValue();
            if (numShards < 1) {
                throw new IllegalArgumentException("num_shards in CLUSTERED clause must be greater than 0");
            }
        } else {
            numShards = Constants.DEFAULT_NUM_SHARDS;
        }
        context.tableParameter().settingsBuilder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards);
        return null;
    }

    @Override
    public Void visitPartitionedBy(PartitionedBy node, CreateTableAnalyzedStatement context) {
        for (Expression partitionByColumn : node.columns()) {
            ColumnIdent partitionedByIdent = ColumnIdent.fromPath(
                    ExpressionToStringVisitor.convert(partitionByColumn, context.parameters()));
            context.analyzedTableElements().changeToPartitionedByColumn(partitionedByIdent, false);
            ColumnIdent routing = context.routing();
            if (routing != null && routing.equals(partitionedByIdent)) {
                throw new IllegalArgumentException(
                        "Cannot use CLUSTERED BY column in PARTITIONED BY clause");
            }
        }
        return null;
    }
}
