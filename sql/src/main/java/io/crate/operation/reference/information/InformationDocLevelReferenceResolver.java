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

package io.crate.operation.reference.information;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.*;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.information.*;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.reference.DocLevelReferenceResolver;
import io.crate.operation.reference.sys.job.RowContextDocLevelReferenceResolver;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.List;
import java.util.Map;

@Singleton
public class InformationDocLevelReferenceResolver implements DocLevelReferenceResolver<RowCollectExpression<?, ?>> {

    private final Map<TableIdent, Map<ColumnIdent, RowCollectExpressionFactory>> factoryMap;
    private final BytesRef DOC_SCHEMA_INFO = new BytesRef(ReferenceInfos.DEFAULT_SCHEMA_NAME);

    @Inject
    public InformationDocLevelReferenceResolver() {
        ImmutableMap.Builder<TableIdent, Map<ColumnIdent, RowCollectExpressionFactory>> builder = ImmutableMap.builder();
        addInformationSchemaTablesFactories(builder);
        addInformationSchemaColumnsFactories(builder);
        addInformationSchemaTablePartitionsFactories(builder);
        addInformationSchemaTableConstraintsFactories(builder);
        addInformationSchemaRoutinesFactories(builder);
        addInformationSchemaSchemataFactories(builder);
        factoryMap = builder.build();
    }

    private void addInformationSchemaSchemataFactories(
            ImmutableMap.Builder<TableIdent, Map<ColumnIdent, RowCollectExpressionFactory>> builder) {

        builder.put(InformationSchemataTableInfo.IDENT, ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
                .put(InformationSchemataTableInfo.Columns.SCHEMA_NAME, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new SchemataSchemaNameExpression();
                    }
                })
                .build());
    }

    private void addInformationSchemaRoutinesFactories(
            ImmutableMap.Builder<TableIdent, Map<ColumnIdent, RowCollectExpressionFactory>> builder) {

        builder.put(InformationRoutinesTableInfo.IDENT, ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
                .put(InformationRoutinesTableInfo.Columns.ROUTINE_NAME, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new InformationRoutinesExpression.RoutineNameExpression();
                    }
                })
                .put(InformationRoutinesTableInfo.Columns.ROUTINE_TYPE, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new InformationRoutinesExpression.RoutineTypeExpression();
                    }
                })
                .build());
    }

    private void addInformationSchemaTableConstraintsFactories(
            ImmutableMap.Builder<TableIdent, Map<ColumnIdent, RowCollectExpressionFactory>> builder) {

        builder.put(InformationTableConstraintsTableInfo.IDENT, ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
                .put(InformationTableConstraintsTableInfo.Columns.SCHEMA_NAME, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new InformationTableConstraintsExpression.TableConstraintsSchemaNameExpression();
                    }
                })
                .put(InformationTableConstraintsTableInfo.Columns.TABLE_NAME, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new InformationTableConstraintsExpression.TableConstraintsTableNameExpression();
                    }
                })
                .put(InformationTableConstraintsTableInfo.Columns.CONSTRAINT_NAME, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new InformationTableConstraintsExpression.TableConstraintsConstraintNameExpression();
                    }
                })
                .put(InformationTableConstraintsTableInfo.Columns.CONSTRAINT_TYPE, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new InformationTableConstraintsExpression.TableConstraintsConstraintTypeExpression();
                    }
                })
                .build());
    }

    private void addInformationSchemaTablePartitionsFactories(
            ImmutableMap.Builder<TableIdent, Map<ColumnIdent, RowCollectExpressionFactory>> builder) {

        builder.put(InformationPartitionsTableInfo.IDENT, ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
                .put(InformationPartitionsTableInfo.Columns.TABLE_NAME, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new InformationTablePartitionsExpression.PartitionsTableNameExpression();
                    }
                })
                .put(InformationPartitionsTableInfo.Columns.SCHEMA_NAME, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new InformationTablePartitionsExpression.PartitionsSchemaNameExpression();
                    }
                })
                .put(InformationPartitionsTableInfo.Columns.PARTITION_IDENT, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new InformationTablePartitionsExpression.PartitionsPartitionIdentExpression();
                    }
                })
                .put(InformationPartitionsTableInfo.Columns.VALUES, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new InformationTablePartitionsExpression.PartitionsValuesExpression();
                    }
                })
                .put(InformationPartitionsTableInfo.Columns.NUMBER_OF_SHARDS, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new InformationTablePartitionsExpression.PartitionsNumberOfShardsExpression();
                    }
                })
                .put(InformationPartitionsTableInfo.Columns.NUMBER_OF_REPLICAS, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new InformationTablePartitionsExpression.PartitionsNumberOfReplicasExpression();
                    }
                })
                .build());
    }

    private void addInformationSchemaColumnsFactories(ImmutableMap.Builder<TableIdent, Map<ColumnIdent, RowCollectExpressionFactory>> builder) {
        builder.put(InformationColumnsTableInfo.IDENT, ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
                .put(InformationColumnsTableInfo.Columns.SCHEMA_NAME, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new InformationColumnsExpression.ColumnsSchemaNameExpression();
                    }
                })
                .put(InformationColumnsTableInfo.Columns.TABLE_NAME, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new InformationColumnsExpression.ColumnsTableNameExpression();
                    }
                })
                .put(InformationColumnsTableInfo.Columns.COLUMN_NAME, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new InformationColumnsExpression.ColumnsColumnNameExpression();
                    }
                })
                .put(InformationColumnsTableInfo.Columns.ORDINAL_POSITION, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new InformationColumnsExpression.ColumnsOrdinalExpression();
                    }
                })
                .put(InformationColumnsTableInfo.Columns.DATA_TYPE, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new InformationColumnsExpression.ColumnsDataTypeExpression();
                    }
                })
                .build());
    }

    private void addInformationSchemaTablesFactories(ImmutableMap.Builder<TableIdent, Map<ColumnIdent, RowCollectExpressionFactory>> builder) {
        builder.put(InformationTablesTableInfo.IDENT, ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
                .put(InformationTablesTableInfo.Columns.SCHEMA_NAME, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<TableInfo, BytesRef>() {

                            @Override
                            public BytesRef value() {
                                String schema = row.ident().schema();
                                if (schema == null) {
                                    return DOC_SCHEMA_INFO;
                                }
                                return new BytesRef(schema);
                            }
                        };
                    }
                })
                .put(InformationTablesTableInfo.Columns.TABLE_NAME, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<TableInfo, BytesRef>() {

                            @Override
                            public BytesRef value() {
                                return new BytesRef(row.ident().name());
                            }
                        };
                    }
                })
                .put(InformationTablesTableInfo.Columns.NUMBER_OF_SHARDS, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<TableInfo, Integer>() {

                            @Override
                            public Integer value() {
                                return row.numberOfShards();
                            }
                        };
                    }
                })
                .put(InformationTablesTableInfo.Columns.NUMBER_OF_REPLICAS, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<TableInfo, BytesRef>() {

                            @Override
                            public BytesRef value() {
                                return row.numberOfReplicas();
                            }
                        };
                    }
                })
                .put(InformationTablesTableInfo.Columns.CLUSTERED_BY, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<TableInfo, BytesRef>() {

                            @Override
                            public BytesRef value() {
                                ColumnIdent clusteredBy = row.clusteredBy();
                                if (clusteredBy == null) {
                                    return null;
                                }
                                return new BytesRef(clusteredBy.fqn());
                            }
                        };
                    }
                })
                .put(InformationTablesTableInfo.Columns.PARTITIONED_BY, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<TableInfo, BytesRef[]>() {

                            @Override
                            public BytesRef[] value() {
                                List<ColumnIdent> partitionedBy = row.partitionedBy();
                                if (partitionedBy == null || partitionedBy.isEmpty()) {
                                    return null;
                                }

                                BytesRef[] partitions = new BytesRef[partitionedBy.size()];
                                for (int i = 0; i < partitions.length; i++) {
                                    partitions[i] = new BytesRef(partitionedBy.get(i).fqn());
                                }
                                return partitions;
                            }
                        };
                    }
                })
                .put(InformationTablesTableInfo.Columns.BLOBS_PATH, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<TableInfo, BytesRef>() {

                            @Override
                            public BytesRef value() {
                                if (row instanceof BlobTableInfo) {
                                    return ((BlobTableInfo) row).blobsPath();
                                }
                                return null;
                            }
                        };
                    }
                })
                .build());
    }

    @Override
    public RowCollectExpression<?, ?> getImplementation(ReferenceInfo info) {
        return RowContextDocLevelReferenceResolver.rowCollectExpressionFromFactoryMap(factoryMap, info);
    }
}
