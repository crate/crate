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
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.RowCollectExpression;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.information.*;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.ShardedTable;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.collect.files.SqlFeatureContext;
import io.crate.operation.reference.partitioned.PartitionsSettingsExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.List;
import java.util.Map;

public class InformationSchemaExpressionFactories {

    private InformationSchemaExpressionFactories() {}

    public static Map<ColumnIdent, RowCollectExpressionFactory> schemataFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
                .put(InformationSchemataTableInfo.Columns.SCHEMA_NAME, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new SchemataSchemaNameExpression();
                    }
                }).build();
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory> routineFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
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
                }).build();
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory> tableConstraintFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
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
                }).build();
    }


    public static Map<ColumnIdent, RowCollectExpressionFactory> tablePartitionsFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
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
                .put(InformationPartitionsTableInfo.Columns.TABLE_SETTINGS, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new PartitionsSettingsExpression();
                    }
                })
                .build();
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory> columnsFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
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
                .put(InformationColumnsTableInfo.Columns.IS_GENERATED, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new InformationColumnsExpression<Boolean>() {
                            @Override
                            public Boolean value() {
                                return row.info instanceof GeneratedReference;
                            }
                        };
                    }
                })
                .put(InformationColumnsTableInfo.Columns.IS_NULLABLE, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new InformationColumnsExpression.ColumnsIsNullableExpression();
                    }
                })
                .put(InformationColumnsTableInfo.Columns.GENERATION_EXPRESSION, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new InformationColumnsExpression<BytesRef>() {
                            @Override
                            public BytesRef value() {
                                if (row.info instanceof GeneratedReference) {
                                    return BytesRefs.toBytesRef(((GeneratedReference) row.info).formattedGeneratedExpression());
                                }
                                return null;
                            }
                        };
                    }
                }).build();
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory> tablesFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
                .put(InformationTablesTableInfo.Columns.SCHEMA_NAME, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<TableInfo, BytesRef>() {

                            @Override
                            public BytesRef value() {
                                return new BytesRef(row.ident().schema());
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
                                if (row instanceof ShardedTable) {
                                    return ((ShardedTable) row).numberOfShards();
                                }
                                return 1;
                            }
                        };
                    }
                })
                .put(InformationTablesTableInfo.Columns.NUMBER_OF_REPLICAS, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<TableInfo, BytesRef>() {
                            private final BytesRef ZERO_REPLICAS = new BytesRef("0");

                            @Override
                            public BytesRef value() {
                                if (row instanceof ShardedTable) {
                                    return ((ShardedTable) row).numberOfReplicas();
                                }
                                return ZERO_REPLICAS;
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
                                        if (row instanceof ShardedTable) {
                                            ColumnIdent clusteredBy = ((ShardedTable) row).clusteredBy();
                                            if (clusteredBy == null) {
                                                return null;
                                            }
                                            return new BytesRef(clusteredBy.fqn());
                                        }
                                        return null;
                                    }
                                };
                            }
                        }
                )
                .put(InformationTablesTableInfo.Columns.PARTITIONED_BY, new RowCollectExpressionFactory() {
                            @Override
                            public RowCollectExpression create() {
                                return new RowContextCollectorExpression<TableInfo, BytesRef[]>() {

                                    @Override
                                    public BytesRef[] value() {
                                        if (row instanceof DocTableInfo) {
                                            List<ColumnIdent> partitionedBy = ((DocTableInfo) row).partitionedBy();
                                            if (partitionedBy == null || partitionedBy.isEmpty()) {
                                                return null;
                                            }

                                            BytesRef[] partitions = new BytesRef[partitionedBy.size()];
                                            for (int i = 0; i < partitions.length; i++) {
                                                partitions[i] = new BytesRef(partitionedBy.get(i).fqn());
                                            }
                                            return partitions;
                                        }
                                        return null;
                                    }
                                };
                            }
                        }
                )
                .put(InformationTablesTableInfo.Columns.COLUMN_POLICY, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<TableInfo, BytesRef>() {

                            @Override
                            public BytesRef value() {
                                if (row instanceof DocTableInfo) {
                                    return new BytesRef(((DocTableInfo) row).columnPolicy().value());
                                }
                                return new BytesRef(ColumnPolicy.STRICT.value());
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
                        }
                )
                .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS, new RowCollectExpressionFactory() {
                            @Override
                            public RowCollectExpression create() {
                                return new TablesSettingsExpression();
                            }
                        }
                ).build();
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory> sqlFeaturesFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
                .put(InformationSqlFeaturesTableInfo.Columns.FEATURE_ID, new RowCollectExpressionFactory() {

                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SqlFeatureContext, BytesRef>() {

                            @Override
                            public BytesRef value() {
                                return BytesRefs.toBytesRef(row.featureId);
                            }
                        };
                    }
                })
                .put(InformationSqlFeaturesTableInfo.Columns.FEATURE_NAME, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SqlFeatureContext, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return BytesRefs.toBytesRef(row.featureName);
                            }
                        };
                    }
                })
                .put(InformationSqlFeaturesTableInfo.Columns.SUB_FEATURE_ID, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SqlFeatureContext, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return BytesRefs.toBytesRef(row.subFeatureId);
                            }
                        };
                    }
                })
                .put(InformationSqlFeaturesTableInfo.Columns.SUB_FEATURE_NAME, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SqlFeatureContext, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return BytesRefs.toBytesRef(row.subFeatureName);
                            }
                        };
                    }
                })
                .put(InformationSqlFeaturesTableInfo.Columns.IS_SUPPORTED, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SqlFeatureContext, Boolean>() {
                            @Override
                            public Boolean value() {
                                return row.isSupported;
                            }
                        };
                    }
                })
                .put(InformationSqlFeaturesTableInfo.Columns.IS_VERIFIED_BY, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SqlFeatureContext, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return BytesRefs.toBytesRef(row.isVerifiedBy);
                            }
                        };
                    }
                })
                .put(InformationSqlFeaturesTableInfo.Columns.COMMENTS, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SqlFeatureContext, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return BytesRefs.toBytesRef(row.comments);
                            }
                        };
                    }
                })
                .build();
    }
}
