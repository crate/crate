/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.metadata.information;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.NestableInput;
import io.crate.expression.reference.MapLookupByPathExpression;
import io.crate.expression.reference.partitioned.PartitionsSettingsExpression;
import io.crate.expression.reference.partitioned.PartitionsVersionExpression;
import io.crate.expression.symbol.DynamicReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexMappings;
import io.crate.metadata.PartitionInfo;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.elasticsearch.Version;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

public class InformationPartitionsTableInfo extends InformationTableInfo {

    public static final String NAME = "table_partitions";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        static final ColumnIdent TABLE_SCHEMA = new ColumnIdent("table_schema");
        static final ColumnIdent VALUES = new ColumnIdent("values");
        static final ColumnIdent PARTITION_IDENT = new ColumnIdent("partition_ident");
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<PartitionInfo>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<PartitionInfo>>builder()
            .put(InformationTablesTableInfo.Columns.TABLE_NAME,
                () -> NestableCollectExpression.forFunction(r -> r.name().relationName().name()))
            .put(Columns.TABLE_SCHEMA,
                () -> NestableCollectExpression.forFunction(r -> r.name().relationName().schema()))
            .put(InformationTablesTableInfo.Columns.TABLE_TYPE,
                () -> NestableCollectExpression.forFunction(r -> RelationType.BASE_TABLE.pretty()))
            .put(Columns.PARTITION_IDENT,
                () -> NestableCollectExpression.forFunction(r -> r.name().ident()))
            .put(Columns.VALUES, () -> new NestableCollectExpression<PartitionInfo, Map<String, Object>>() {
                private Map<String, Object> value;

                @Override
                public void setNextRow(PartitionInfo row) {
                    value = row.values();
                }

                @Override
                public Map<String, Object> value() {
                    return value;
                }

                @Override
                public NestableInput getChild(String name) {
                    // The input values could be of mixed types (select values['p'];  t1 (p int); t2 (p string))
                    // The result is casted to string because streaming (via pg) doesn't support mixed types;
                    return new MapLookupByPathExpression<>(
                        PartitionInfo::values, Collections.singletonList(name), DataTypes.STRING::value);
                }
            })
            .put(InformationTablesTableInfo.Columns.NUMBER_OF_SHARDS,
                () -> NestableCollectExpression.forFunction(PartitionInfo::numberOfShards))
            .put(InformationTablesTableInfo.Columns.NUMBER_OF_REPLICAS,
                () -> NestableCollectExpression.forFunction(PartitionInfo::numberOfReplicas))
            .put(InformationTablesTableInfo.Columns.ROUTING_HASH_FUNCTION,
                () -> NestableCollectExpression.forFunction(r -> IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME))
            .put(InformationTablesTableInfo.Columns.CLOSED,
                () -> NestableCollectExpression.forFunction(PartitionInfo::isClosed))
            .put(InformationTablesTableInfo.Columns.TABLE_VERSION, PartitionsVersionExpression::new)
            .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS, PartitionsSettingsExpression::new)
            .build();
    }

    private static ColumnRegistrar createColumnRegistrar() {
        return new ColumnRegistrar(IDENT, RowGranularity.DOC)
            .register(Columns.TABLE_SCHEMA, DataTypes.STRING)
            .register(InformationTablesTableInfo.Columns.TABLE_NAME, DataTypes.STRING)
            .register(Columns.PARTITION_IDENT, DataTypes.STRING)
            .register(Columns.VALUES, ObjectType.untyped())
            .register(InformationTablesTableInfo.Columns.NUMBER_OF_SHARDS, DataTypes.INTEGER)
            .register(InformationTablesTableInfo.Columns.NUMBER_OF_REPLICAS, DataTypes.STRING)
            .register(InformationTablesTableInfo.Columns.ROUTING_HASH_FUNCTION, DataTypes.STRING)
            .register(InformationTablesTableInfo.Columns.CLOSED, DataTypes.BOOLEAN)
            .register(InformationTablesTableInfo.Columns.TABLE_VERSION, ObjectType.builder()
                .setInnerType(Version.Property.CREATED.toString(), DataTypes.STRING)
                .setInnerType(Version.Property.UPGRADED.toString(), DataTypes.STRING)
                .build())
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS, ObjectType.builder()
                .setInnerType("blocks", ObjectType.builder()
                    .setInnerType("read_only", DataTypes.BOOLEAN)
                    .setInnerType("read", DataTypes.BOOLEAN)
                    .setInnerType("write", DataTypes.BOOLEAN)
                    .setInnerType("metadata", DataTypes.BOOLEAN)
                    .build())
                .setInnerType("translog", ObjectType.builder()
                    .setInnerType("flush_threshold_size", DataTypes.LONG)
                    .setInnerType("sync_interval", DataTypes.LONG)
                    .build())
                .setInnerType("routing", ObjectType.builder()
                    .setInnerType("allocation", ObjectType.builder()
                        .setInnerType("enable", DataTypes.STRING)
                        .setInnerType("total_shards_per_node", DataTypes.INTEGER)
                        .build())
                    .build())
                .setInnerType("warmer", ObjectType.builder()
                    .setInnerType("enabled", DataTypes.BOOLEAN)
                    .build())
                .setInnerType("unassigned", ObjectType.builder()
                    .setInnerType("node_left", ObjectType.builder()
                        .setInnerType("delayed_timeout", DataTypes.LONG)
                        .build())
                    .build())
                .setInnerType("mapping", ObjectType.builder()
                    .setInnerType("total_fields", ObjectType.builder()
                        .setInnerType("limit", DataTypes.INTEGER)
                        .build())
                    .build())
                .build());
    }

    InformationPartitionsTableInfo() {
        super(
            IDENT,
            createColumnRegistrar(),
            ImmutableList.of(Columns.TABLE_SCHEMA, InformationTablesTableInfo.Columns.TABLE_NAME, Columns.PARTITION_IDENT)
        );
    }

    @Nullable
    @Override
    public Reference getReference(ColumnIdent column) {
        if (!column.isTopLevel() && column.name().equals(Columns.VALUES.name())) {
            DynamicReference ref = new DynamicReference(new ReferenceIdent(ident(), column), rowGranularity());
            ref.valueType(DataTypes.STRING);
            return ref;
        }
        return super.getReference(column);
    }
}
