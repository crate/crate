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

package io.crate.metadata.table;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.crate.Version;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.TableReferenceResolver;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Expression;
import io.crate.types.DataType;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class TestingTableInfo extends DocTableInfo {

    private Routing routing;

    public TestingTableInfo(RelationName ident,
                            List<Reference> columns,
                            List<Reference> partitionedByColumns,
                            List<GeneratedReference> generatedColumns,
                            ImmutableMap<ColumnIdent, IndexReference> indexColumns,
                            ImmutableMap<ColumnIdent, Reference> references,
                            List<ColumnIdent> primaryKeys,
                            ColumnIdent clusteredBy,
                            boolean hasAutoGeneratedPrimaryKey,
                            String[] concreteIndices,
                            int numberOfShards,
                            String numberOfReplicas,
                            ImmutableMap<String, Object> tableParameters,
                            List<ColumnIdent> partitionedBy,
                            List<PartitionName> partitions,
                            ColumnPolicy columnPolicy, Routing routing) {
        super(ident, columns, partitionedByColumns, generatedColumns, ImmutableList.of(), indexColumns, references,
              ImmutableMap.of(), primaryKeys, clusteredBy,
              hasAutoGeneratedPrimaryKey, concreteIndices, concreteIndices, new IndexNameExpressionResolver(Settings.EMPTY),
              numberOfShards, numberOfReplicas, tableParameters, partitionedBy, partitions, columnPolicy,
              Version.CURRENT, null, false,
              Operation.ALL);
        this.routing = routing;
    }

    @Override
    public Routing getRouting(ClusterState state,
                              RoutingProvider routingProvider,
                              WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              SessionContext sessionContext) {
        return routing;
    }

    public static Builder builder(RelationName ident, Routing routing) {
        return new Builder(ident, routing);
    }

    public static class Builder {

        private final ImmutableList.Builder<Reference> columns = ImmutableList.builder();
        private final ImmutableMap.Builder<ColumnIdent, Reference> references = ImmutableMap.builder();
        private final ImmutableList.Builder<Reference> partitionedByColumns = ImmutableList.builder();
        private final ImmutableList.Builder<GeneratedReference> generatedColumns = ImmutableList.builder();
        private final ImmutableList.Builder<ColumnIdent> primaryKey = ImmutableList.builder();
        private final ImmutableList.Builder<ColumnIdent> partitionedBy = ImmutableList.builder();
        private final ImmutableList.Builder<PartitionName> partitions = ImmutableList.builder();
        private final ImmutableMap.Builder<ColumnIdent, IndexReference> indexColumns = ImmutableMap.builder();
        private ColumnIdent clusteredBy;

        private final int numberOfShards = 1;
        private final String numberOfReplicas = "0";

        private final RelationName ident;
        private final Routing routing;
        private ColumnPolicy columnPolicy = ColumnPolicy.DYNAMIC;

        public Builder(RelationName ident, Routing routing) {
            this.routing = routing;
            this.ident = ident;
        }

        public DocTableInfo build() {
            return build(mock(Functions.class));
        }

        public DocTableInfo build(Functions functions) {
            addDocSysColumns();
            ImmutableList<ColumnIdent> pk = primaryKey.build();
            ImmutableList<PartitionName> partitionsList = partitions.build();
            initializeGeneratedExpressions(functions, references.build().values());

            return new TestingTableInfo(
                ident,
                columns.build(),
                partitionedByColumns.build(),
                generatedColumns.build(),
                indexColumns.build(),
                references.build(),
                pk,
                routingColumn(clusteredBy, pk),
                pk.isEmpty(),
                concreteIndices(ident, partitionsList),
                numberOfShards,
                numberOfReplicas,
                null, // tableParameters
                partitionedBy.build(),
                partitionsList,
                columnPolicy,
                routing
            );
        }

        private static String[] concreteIndices(RelationName ident, ImmutableList<PartitionName> partitionsList) {
            String[] concreteIndices;
            if (partitionsList.isEmpty()) {
                concreteIndices = new String[]{ident.indexName()};
            } else {
                concreteIndices = Lists.transform(partitionsList, new Function<PartitionName, String>() {
                    @Nullable
                    @Override
                    public String apply(@Nullable PartitionName input) {
                        assert input != null;
                        return input.asIndexName();
                    }
                }).toArray(new String[partitionsList.size()]);
            }
            return concreteIndices;
        }

        private static ColumnIdent routingColumn(@Nullable ColumnIdent clusteredBy, ImmutableList<ColumnIdent> primaryKey) {
            if (clusteredBy != null) {
                return clusteredBy;
            }
            if (primaryKey.size() == 1) {
                return primaryKey.get(0);
            }
            return null;
        }

        private Reference genInfo(ColumnIdent columnIdent, DataType type) {
            return new Reference(
                new ReferenceIdent(ident, columnIdent.name(), columnIdent.path()),
                RowGranularity.DOC, type
            );
        }

        private void addDocSysColumns() {
            for (Map.Entry<ColumnIdent, DataType> entry : DocSysColumns.COLUMN_IDENTS.entrySet()) {
                references.put(
                    entry.getKey(),
                    genInfo(entry.getKey(), entry.getValue())
                );
            }
        }

        public Builder add(String column, DataType type) {
            return add(column, type, null);
        }

        public Builder add(String column, DataType type, List<String> path) {
            return add(column, type, path, ColumnPolicy.DYNAMIC);
        }

        public Builder add(String column, DataType type, List<String> path, ColumnPolicy columnPolicy) {
            return add(column, type, path, columnPolicy, Reference.IndexType.NOT_ANALYZED, false, true);
        }

        public Builder add(String column, DataType type, List<String> path,
                           boolean partitionBy) {
            return add(column, type, path, ColumnPolicy.DYNAMIC,
                Reference.IndexType.NOT_ANALYZED, partitionBy, true);
        }

        public Builder add(String column, DataType type, List<String> path,
                           ColumnPolicy columnPolicy, Reference.IndexType indexType,
                           boolean partitionBy,
                           boolean nullable) {
            RowGranularity rowGranularity = RowGranularity.DOC;
            if (partitionBy) {
                rowGranularity = RowGranularity.PARTITION;
            }
            Reference ref = new Reference(new ReferenceIdent(ident, column, path),
                rowGranularity, type, columnPolicy, indexType, nullable);
            if (ref.column().isTopLevel()) {
                columns.add(ref);
            }
            references.put(ref.column(), ref);
            if (partitionBy) {
                partitionedByColumns.add(ref);
                partitionedBy.add(ref.column());
            }
            return this;
        }

        public Builder addGeneratedColumn(String column, DataType type, String expression, boolean partitionBy) {
            return addGeneratedColumn(column, type, expression, partitionBy, true);
        }

        public Builder addGeneratedColumn(String column, DataType type, String expression,
                                          boolean partitionBy, boolean nullable) {
            RowGranularity rowGranularity = RowGranularity.DOC;
            if (partitionBy) {
                rowGranularity = RowGranularity.PARTITION;
            }
            GeneratedReference ref = new GeneratedReference(new ReferenceIdent(ident, column),
                rowGranularity, type, ColumnPolicy.DYNAMIC, Reference.IndexType.NOT_ANALYZED, expression, nullable);

            generatedColumns.add(ref);
            if (ref.column().isTopLevel()) {
                columns.add(ref);
            }
            references.put(ref.column(), ref);
            if (partitionBy) {
                partitionedByColumns.add(ref);
                partitionedBy.add(ref.column());
            }
            return this;
        }

        public Builder addPrimaryKey(String column) {
            primaryKey.add(ColumnIdent.fromPath(column));
            return this;
        }

        public Builder clusteredBy(String clusteredBy) {
            this.clusteredBy = ColumnIdent.fromPath(clusteredBy);
            return this;
        }

        public Builder addPartitions(String... partitionNames) {
            for (String partitionName : partitionNames) {
                PartitionName partition = PartitionName.fromIndexOrTemplate(partitionName);
                partitions.add(partition);
            }
            return this;
        }

        private void initializeGeneratedExpressions(Functions functions, Collection<Reference> columns) {
            TransactionContext transactionContext = TransactionContext.systemTransactionContext();
            TableReferenceResolver tableReferenceResolver = new TableReferenceResolver(columns, ident);
            ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
                functions, transactionContext, null, tableReferenceResolver, null);
            for (GeneratedReference generatedReferenceInfo : generatedColumns.build()) {
                Expression expression = SqlParser.createExpression(generatedReferenceInfo.formattedGeneratedExpression());
                ExpressionAnalysisContext context = new ExpressionAnalysisContext();
                generatedReferenceInfo.generatedExpression(expressionAnalyzer.convert(expression, context));
                generatedReferenceInfo.referencedReferences(ImmutableList.copyOf(tableReferenceResolver.references()));
                tableReferenceResolver.references().clear();
            }
        }

    }
}
