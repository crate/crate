/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;

import io.crate.analyze.TableElementsAnalyzer.RefBuilder;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.IndexReference;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.operators.SubQueryAndParamBinder;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.PartitionedBy;

public record AnalyzedCreateTable(
        RelationName relationName,
        boolean ifNotExists,
        /**
         * In order of definition
         */
        Map<ColumnIdent, RefBuilder> columns,
        /**
         * By constraint name; In order of definition
         **/
        Map<String, AnalyzedCheck> checks,
        GenericProperties<Symbol> properties,
        Optional<PartitionedBy<Symbol>> partitionedBy,
        Optional<ClusteredBy<Symbol>> clusteredBy) implements DDLStatement {

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        for (var refBuilder : columns.values()) {
            refBuilder.visitSymbols(consumer);
        }
        for (AnalyzedCheck check : checks.values()) {
            consumer.accept(check.check());
        }
        properties.properties().values().forEach(consumer);
        partitionedBy.ifPresent(x -> x.columns().forEach(consumer));
        clusteredBy.ifPresent(x -> {
            x.column().ifPresent(consumer);
            x.numberOfShards().ifPresent(consumer);
        });
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitCreateTable(this, context);
    }

    public BoundCreateTable bind(NumberOfShards numberOfShards,
                                 FulltextAnalyzerResolver fulltextAnalyzerResolver,
                                 NodeContext nodeCtx,
                                 TransactionContext txnCtx,
                                 Row params,
                                 SubQueryResults subQueryResults) {
        SubQueryAndParamBinder paramBinder = new SubQueryAndParamBinder(params, subQueryResults);
        SymbolEvaluator evaluator = new SymbolEvaluator(txnCtx, nodeCtx, subQueryResults);
        Function<Symbol, Object> toValue = evaluator.bind(params);

        Integer numShards = clusteredBy
            .flatMap(ClusteredBy::numberOfShards)
            .map(toValue)
            .map(numberOfShards::fromNumberOfShards)
            .orElseGet(numberOfShards::defaultNumberOfShards);

        TableParameter tableParameter = new TableParameter();
        tableParameter.settingsBuilder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards);

        Map<ColumnIdent, Reference> references = new LinkedHashMap<>();
        LinkedHashSet<Reference> primaryKeys = new LinkedHashSet<>();
        Set<String> pkConstraintNames = new HashSet<>();
        for (var entry : columns.entrySet()) {
            ColumnIdent columnIdent = entry.getKey();
            RefBuilder column = entry.getValue();
            Reference reference = column.build(columns, relationName, paramBinder, toValue);
            references.put(columnIdent, reference);
            if (column.isPrimaryKey()) {
                primaryKeys.add(reference);
                pkConstraintNames.add(column.pkConstraintName());
            }
            if (reference instanceof IndexReference indexRef) {
                String analyzer = indexRef.analyzer();
                if (fulltextAnalyzerResolver.hasCustomAnalyzer(analyzer)) {
                    Settings settings = fulltextAnalyzerResolver.resolveFullCustomAnalyzerSettings(analyzer);
                    tableParameter.settingsBuilder().put(settings);
                }
            }
        }
        String pkConstraintName = null;
        if (!pkConstraintNames.isEmpty()) {
            // CrateDB allows 'create table t (a int constraint c1 primary key, b int constraint c2 primary key);',
            // making sure that c1 == c2.
            if (pkConstraintNames.size() > 1) {
                throw new IllegalArgumentException(
                    "More than one name for PRIMARY KEY constraint provided: " + String.join(",", pkConstraintNames));
            }
            pkConstraintName = pkConstraintNames.iterator().next();
            if ("".equals(pkConstraintName)) {
                throw new IllegalArgumentException(
                    "The name of primary key constraint must not be empty, please either use a name or remove the CONSTRAINT keyword");
            }
        }

        TableProperties.analyze(
            tableParameter, TableParameters.TABLE_CREATE_PARAMETER_INFO, properties.map(toValue), true);

        Optional<ColumnIdent> optClusteredBy = clusteredBy
            .flatMap(ClusteredBy::column)
            .map(Symbols::pathFromSymbol);
        optClusteredBy.ifPresent(c -> {
            if (!primaryKeys.isEmpty() && Reference.indexOf(primaryKeys, c) < 0) {
                throw new IllegalArgumentException(
                    "Clustered by column `" + c + "` must be part of primary keys: " + Lists2.map(primaryKeys, Reference::column));
            }
        });
        ColumnIdent routingColumn = optClusteredBy.orElse(DocSysColumns.ID);

        List<Symbol> partitionedByColumns = partitionedBy
            .map(PartitionedBy::columns)
            .orElse(List.of());

        return new BoundCreateTable(
            relationName,
            pkConstraintName,
            ifNotExists,
            references,
            tableParameter,
            List.copyOf(primaryKeys),
            checks,
            routingColumn,
            partitionedByColumns
        );
    }
}
