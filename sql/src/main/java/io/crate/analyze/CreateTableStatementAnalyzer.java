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
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.TableIdent;
import io.crate.sql.tree.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;

import java.util.Locale;


public class CreateTableStatementAnalyzer extends AbstractStatementAnalyzer<Void, CreateTableAnalysis> {

    protected Void visitNode(Node node, CreateTableAnalysis context) {
        throw new RuntimeException(
                String.format("Encountered node %s but expected a CreateTable node", node));
    }

    @Override
    public Void visitCreateTable(CreateTable node, CreateTableAnalysis context) {
        TableIdent tableIdent = TableIdent.of(node.name());
        Preconditions.checkArgument(Strings.isNullOrEmpty(tableIdent.schema()),
                "A custom schema name must not be specified in the CREATE TABLE clause");
        context.table(tableIdent);

        // apply default in case it is not specified in the genericProperties,
        // if it is it will get overwritten afterwards.
        if (node.properties().isPresent()) {
            Settings settings =
                    TablePropertiesAnalysis.propertiesToSettings(node.properties().get(), context.parameters(), true);
            context.indexSettingsBuilder().put(settings);
        }

        context.analyzedTableElements(TableElementsAnalyzer.analyze(
                node.tableElements(),
                context.parameters(),
                context.fulltextAnalyzerResolver()));

        context.analyzedTableElements().finalizeAndValidate();

        for (CrateTableOption option : node.crateTableOptions()) {
            process(option, context);
        }
        return null;
    }

    @Override
    public Void visitClusteredBy(ClusteredBy node, CreateTableAnalysis context) {
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
        int numShards = node.numberOfShards().or(Constants.DEFAULT_NUM_SHARDS);
        if (numShards < 1) {
            throw new IllegalArgumentException("num_shards in CLUSTERED clause must be greater than 0");
        }
        context.indexSettingsBuilder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards);
        return null;
    }

    @Override
    public Void visitPartitionedBy(PartitionedBy node, CreateTableAnalysis context) {
        for (Expression partitionByColumn : node.columns()) {
            ColumnIdent partitionedByIdent = ColumnIdent.fromPath(
                    ExpressionToStringVisitor.convert(partitionByColumn, context.parameters()));
            context.analyzedTableElements().changeToPartitionedByColumn(partitionedByIdent);
            ColumnIdent routing = context.routing();
            if (routing != null && routing.equals(partitionedByIdent)) {
                throw new IllegalArgumentException(
                        "Cannot use CLUSTERED BY column in PARTITIONED BY clause");
            }
        }
        return null;
    }
}
