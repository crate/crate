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
import io.crate.metadata.doc.DocIndexMetaData;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.information.*;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.ShardedTable;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.collect.files.SqlFeatureContext;
import io.crate.operation.reference.partitioned.PartitionsSettingsExpression;
import io.crate.operation.reference.partitioned.PartitionsVersionExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.List;
import java.util.Map;

public class InformationSchemaExpressionFactories {

    private InformationSchemaExpressionFactories() {
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<SchemaInfo>> schemataFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<SchemaInfo>>builder()
            .put(InformationSchemataTableInfo.Columns.SCHEMA_NAME, SchemataSchemaNameExpression::new).build();
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<RoutineInfo>> routineFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<RoutineInfo>>builder()
            .put(InformationRoutinesTableInfo.Columns.ROUTINE_NAME, InformationRoutinesExpression.RoutineNameExpression::new)
            .put(InformationRoutinesTableInfo.Columns.ROUTINE_TYPE, InformationRoutinesExpression.RoutineTypeExpression::new)
            .put(InformationRoutinesTableInfo.Columns.ROUTINE_SCHEMA, InformationRoutinesExpression.RoutineSchemaExpression::new)
            .put(InformationRoutinesTableInfo.Columns.SPECIFIC_NAME, InformationRoutinesExpression.RoutineSpecificNameExpression::new)
            .put(InformationRoutinesTableInfo.Columns.ROUTINE_BODY, InformationRoutinesExpression.RoutineBodyExpression::new)
            .put(InformationRoutinesTableInfo.Columns.ROUTINE_DEFINITION, InformationRoutinesExpression.RoutineDefinitionExpression::new)
            .put(InformationRoutinesTableInfo.Columns.DATA_TYPE, InformationRoutinesExpression.DataTypeExpression::new)
            .put(InformationRoutinesTableInfo.Columns.IS_DETERMINISTIC, InformationRoutinesExpression.IsDeterministicExpression::new).build();
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<TableInfo>> tableConstraintFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<TableInfo>>builder()
            .put(InformationTableConstraintsTableInfo.Columns.TABLE_SCHEMA,
                InformationTableConstraintsExpression.TableConstraintsSchemaNameExpression::new)
            .put(InformationTableConstraintsTableInfo.Columns.TABLE_NAME,
                InformationTableConstraintsExpression.TableConstraintsTableNameExpression::new)
            .put(InformationTableConstraintsTableInfo.Columns.CONSTRAINT_NAME,
                InformationTableConstraintsExpression.TableConstraintsConstraintNameExpression::new)
            .put(InformationTableConstraintsTableInfo.Columns.CONSTRAINT_TYPE,
                InformationTableConstraintsExpression.TableConstraintsConstraintTypeExpression::new).build();
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<PartitionInfo>> tablePartitionsFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<PartitionInfo>>builder()
            .put(InformationPartitionsTableInfo.Columns.TABLE_NAME,
                InformationTablePartitionsExpression.PartitionsTableNameExpression::new)
            .put(InformationPartitionsTableInfo.PartitionsTableColumns.SCHEMA_NAME,
                InformationTablePartitionsExpression.PartitionsSchemaNameExpression::new)
            .put(InformationPartitionsTableInfo.Columns.TABLE_CATALOG,
                InformationTablePartitionsExpression.PartitionsTableCatalogExpression::new)
            .put(InformationPartitionsTableInfo.Columns.TABLE_TYPE,
                InformationTablePartitionsExpression.PartitionsTableTypeExpression::new)
            .put(InformationPartitionsTableInfo.Columns.PARTITION_IDENT,
                InformationTablePartitionsExpression.PartitionsPartitionIdentExpression::new)
            .put(InformationPartitionsTableInfo.Columns.VALUES,
                InformationTablePartitionsExpression.PartitionsValuesExpression::new)
            .put(InformationPartitionsTableInfo.Columns.NUMBER_OF_SHARDS,
                InformationTablePartitionsExpression.PartitionsNumberOfShardsExpression::new)
            .put(InformationPartitionsTableInfo.Columns.NUMBER_OF_REPLICAS,
                InformationTablePartitionsExpression.PartitionsNumberOfReplicasExpression::new)
            .put(InformationPartitionsTableInfo.Columns.ROUTING_HASH_FUNCTION,
                InformationTablePartitionsExpression.PartitionsRoutingHashFunctionExpression::new)
            .put(InformationPartitionsTableInfo.Columns.CLOSED, InformationTablePartitionsExpression.ClosedExpression::new)
            .put(InformationPartitionsTableInfo.Columns.TABLE_VERSION, PartitionsVersionExpression::new)
            .put(InformationPartitionsTableInfo.Columns.TABLE_SETTINGS, PartitionsSettingsExpression::new)
            .put(InformationPartitionsTableInfo.Columns.SELF_REFERENCING_COLUMN_NAME,
                InformationTablePartitionsExpression.PartitionsSelfReferencingColumnNameExpression::new)
            .put(InformationPartitionsTableInfo.Columns.REFERENCE_GENERATION,
                InformationTablePartitionsExpression.PartitionsReferenceGenerationExpression::new).build();
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<ColumnContext>> columnsFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<ColumnContext>>builder()
            .put(InformationColumnsTableInfo.Columns.TABLE_SCHEMA, InformationColumnsExpression.ColumnsSchemaNameExpression::new)
            .put(InformationColumnsTableInfo.Columns.TABLE_NAME, InformationColumnsExpression.ColumnsTableNameExpression::new)
            .put(InformationColumnsTableInfo.Columns.TABLE_CATALOG, InformationColumnsExpression.ColumnsSchemaNameExpression::new)
            .put(InformationColumnsTableInfo.Columns.COLUMN_NAME, InformationColumnsExpression.ColumnsColumnNameExpression::new)
            .put(InformationColumnsTableInfo.Columns.ORDINAL_POSITION, InformationColumnsExpression.ColumnsOrdinalExpression::new)
            .put(InformationColumnsTableInfo.Columns.DATA_TYPE, InformationColumnsExpression.ColumnsDataTypeExpression::new)
            .put(InformationColumnsTableInfo.Columns.COLUMN_DEFAULT, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.CHARACTER_MAXIMUM_LENGTH, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.CHARACTER_OCTET_LENGTH, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.NUMERIC_PRECISION, InformationColumnsExpression.ColumnsNumericPrecisionExpression::new)
            .put(InformationColumnsTableInfo.Columns.NUMERIC_PRECISION_RADIX, InformationColumnsExpression.ColumnsNumericPrecisionRadixExpression::new)
            .put(InformationColumnsTableInfo.Columns.NUMERIC_SCALE, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.DATETIME_PRECISION, InformationColumnsExpression.ColumnsDatetimePrecisionExpression::new)
            .put(InformationColumnsTableInfo.Columns.INTERVAL_TYPE, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.INTERVAL_PRECISION, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.CHARACTER_SET_CATALOG, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.CHARACTER_SET_SCHEMA, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.CHARACTER_SET_NAME, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.COLLATION_CATALOG, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.COLLATION_SCHEMA, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.COLLATION_NAME, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.DOMAIN_CATALOG, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.DOMAIN_SCHEMA, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.DOMAIN_NAME, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.USER_DEFINED_TYPE_CATALOG, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.USER_DEFINED_TYPE_SCHEMA, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.USER_DEFINED_TYPE_NAME, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.CHECK_REFERENCES, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.CHECK_ACTION, InformationColumnsExpression.ColumnsNullExpression::new)
            .put(InformationColumnsTableInfo.Columns.IS_GENERATED, () -> new InformationColumnsExpression<Boolean>() {
                @Override
                public Boolean value() {
                    return row.info instanceof GeneratedReference;
                }
            })
            .put(InformationColumnsTableInfo.Columns.IS_NULLABLE, InformationColumnsExpression.ColumnsIsNullableExpression::new)
            .put(InformationColumnsTableInfo.Columns.GENERATION_EXPRESSION, () -> new InformationColumnsExpression<BytesRef>() {
                @Override
                public BytesRef value() {
                    if (row.info instanceof GeneratedReference) {
                        return BytesRefs.toBytesRef(((GeneratedReference) row.info).formattedGeneratedExpression());
                    }
                    return null;
                }
            }).build();
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<TableInfo>> tablesFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<TableInfo>>builder()
            .put(InformationTablesTableInfo.Columns.TABLE_SCHEMA, () -> new RowContextCollectorExpression<TableInfo, BytesRef>() {

                @Override
                public BytesRef value() {
                    return new BytesRef(row.ident().schema());
                }
            })
            .put(InformationTablesTableInfo.Columns.TABLE_NAME, () -> new RowContextCollectorExpression<TableInfo, BytesRef>() {

                @Override
                public BytesRef value() {
                    return new BytesRef(row.ident().name());
                }
            })
            .put(InformationTablesTableInfo.Columns.TABLE_CATALOG, () -> new RowContextCollectorExpression<TableInfo, BytesRef>() {

                @Override
                public BytesRef value() {
                    return new BytesRef(row.ident().schema());
                }
            })
            .put(InformationTablesTableInfo.Columns.TABLE_TYPE, () -> new RowContextCollectorExpression<TableInfo, BytesRef>() {

                @Override
                public BytesRef value() {
                    return InformationTablePartitionsExpression.TABLE_TYPE;
                }
            })
            .put(InformationTablesTableInfo.Columns.NUMBER_OF_SHARDS, () -> new RowContextCollectorExpression<TableInfo, Integer>() {

                @Override
                public Integer value() {
                    if (row instanceof ShardedTable) {
                        return ((ShardedTable) row).numberOfShards();
                    }
                    return 1;
                }
            })
            .put(InformationTablesTableInfo.Columns.NUMBER_OF_REPLICAS, () -> new RowContextCollectorExpression<TableInfo, BytesRef>() {
                private final BytesRef ZERO_REPLICAS = new BytesRef("0");

                @Override
                public BytesRef value() {
                    if (row instanceof ShardedTable) {
                        return ((ShardedTable) row).numberOfReplicas();
                    }
                    return ZERO_REPLICAS;
                }
            })
            .put(InformationTablesTableInfo.Columns.CLUSTERED_BY, () -> new RowContextCollectorExpression<TableInfo, BytesRef>() {

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
            }
            )
            .put(InformationTablesTableInfo.Columns.PARTITIONED_BY, () -> new RowContextCollectorExpression<TableInfo, BytesRef[]>() {

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
            }
            )
            .put(InformationTablesTableInfo.Columns.COLUMN_POLICY, () -> new RowContextCollectorExpression<TableInfo, BytesRef>() {

                @Override
                public BytesRef value() {
                    if (row instanceof DocTableInfo) {
                        return new BytesRef(((DocTableInfo) row).columnPolicy().value());
                    }
                    return new BytesRef(ColumnPolicy.STRICT.value());
                }
            })
            .put(InformationTablesTableInfo.Columns.BLOBS_PATH, () -> new RowContextCollectorExpression<TableInfo, BytesRef>() {

                @Override
                public BytesRef value() {
                    if (row instanceof BlobTableInfo) {
                        return ((BlobTableInfo) row).blobsPath();
                    }
                    return null;
                }
            }
            )
            .put(InformationTablesTableInfo.Columns.ROUTING_HASH_FUNCTION,
                () -> new RowContextCollectorExpression<TableInfo, BytesRef>() {
                    @Override
                    public BytesRef value() {
                        if (row instanceof ShardedTable) {
                            return new BytesRef(DocIndexMetaData.getRoutingHashFunctionPrettyName(
                                ((ShardedTable) row).routingHashFunction()));
                        }
                        return null;
                    }
                })
            .put(InformationTablesTableInfo.Columns.CLOSED,
                () -> new RowContextCollectorExpression<TableInfo, Boolean>() {
                    @Override
                    public Boolean value() {
                        if (row instanceof ShardedTable) {
                            return ((ShardedTable) row).isClosed();
                        }
                        return null;
                    }
                })
            .put(InformationTablesTableInfo.Columns.SELF_REFERENCING_COLUMN_NAME, () -> new RowContextCollectorExpression<TableInfo, BytesRef>() {

                @Override
                public BytesRef value() {
                    return InformationTablePartitionsExpression.SELF_REFERENCING_COLUMN_NAME;
                }
            })
            .put(InformationTablesTableInfo.Columns.REFERENCE_GENERATION, () -> new RowContextCollectorExpression<TableInfo, BytesRef>() {

                @Override
                public BytesRef value() {
                    return InformationTablePartitionsExpression.REFERENCE_GENERATION;
                }
            })
            .put(InformationTablesTableInfo.Columns.TABLE_VERSION, TablesVersionExpression::new)
            .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS, TablesSettingsExpression::new
            ).build();
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<SqlFeatureContext>> sqlFeaturesFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<SqlFeatureContext>>builder()
            .put(InformationSqlFeaturesTableInfo.Columns.FEATURE_ID,
                () -> new RowContextCollectorExpression<SqlFeatureContext, BytesRef>() {

                @Override
                public BytesRef value() {
                    return BytesRefs.toBytesRef(row.featureId);
                }
            })
            .put(InformationSqlFeaturesTableInfo.Columns.FEATURE_NAME,
                () -> new RowContextCollectorExpression<SqlFeatureContext, BytesRef>() {

                @Override
                public BytesRef value() {
                    return BytesRefs.toBytesRef(row.featureName);
                }
            })
            .put(InformationSqlFeaturesTableInfo.Columns.SUB_FEATURE_ID,
                () -> new RowContextCollectorExpression<SqlFeatureContext, BytesRef>() {

                @Override
                public BytesRef value() {
                    return BytesRefs.toBytesRef(row.subFeatureId);
                }
            })
            .put(InformationSqlFeaturesTableInfo.Columns.SUB_FEATURE_NAME,
                () -> new RowContextCollectorExpression<SqlFeatureContext, BytesRef>() {

                @Override
                public BytesRef value() {
                    return BytesRefs.toBytesRef(row.subFeatureName);
                }
            })
            .put(InformationSqlFeaturesTableInfo.Columns.IS_SUPPORTED,
                () -> new RowContextCollectorExpression<SqlFeatureContext, Boolean>() {

                @Override
                public Boolean value() {
                    return row.isSupported;
                }
            })
            .put(InformationSqlFeaturesTableInfo.Columns.IS_VERIFIED_BY,
                () -> new RowContextCollectorExpression<SqlFeatureContext, BytesRef>() {

                @Override
                public BytesRef value() {
                    return BytesRefs.toBytesRef(row.isVerifiedBy);
                }
            })
            .put(InformationSqlFeaturesTableInfo.Columns.COMMENTS,
                () -> new RowContextCollectorExpression<SqlFeatureContext, BytesRef>() {

                @Override
                public BytesRef value() {
                    return BytesRefs.toBytesRef(row.comments);
                }
            }).build();
    }
}
