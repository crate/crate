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
import io.crate.expression.symbol.DynamicReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexMappings;
import io.crate.metadata.PartitionInfo;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.expression.NestableInput;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.expression.reference.MapLookupByPathExpression;
import io.crate.expression.reference.partitioned.PartitionsSettingsExpression;
import io.crate.expression.reference.partitioned.PartitionsVersionExpression;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

public class InformationPartitionsTableInfo extends InformationTableInfo {

    public static final String NAME = "table_partitions";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        // TODO: change column name to table_schema so it is equivalent to the information_schema.tables column
        static final ColumnIdent TABLE_SCHEMA = new ColumnIdent("schema_name");
        static final ColumnIdent VALUES = new ColumnIdent("values");
        static final ColumnIdent PARTITION_IDENT = new ColumnIdent("partition_ident");
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<PartitionInfo>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<PartitionInfo>>builder()
            .put(InformationTablesTableInfo.Columns.TABLE_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.name().tableIdent().name()))
            .put(Columns.TABLE_SCHEMA,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.name().tableIdent().schema()))
            .put(InformationTablesTableInfo.Columns.TABLE_TYPE,
                () -> RowContextCollectorExpression.objToBytesRef(r -> RelationType.BASE_TABLE.pretty()))
            .put(Columns.PARTITION_IDENT,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.name().ident()))
            .put(Columns.VALUES, () -> new RowContextCollectorExpression<PartitionInfo, Object>() {

                @Override
                public Object value() {
                    return row.values();
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
                () -> RowContextCollectorExpression.forFunction(PartitionInfo::numberOfShards))
            .put(InformationTablesTableInfo.Columns.NUMBER_OF_REPLICAS,
                () -> RowContextCollectorExpression.objToBytesRef(PartitionInfo::numberOfReplicas))
            .put(InformationTablesTableInfo.Columns.ROUTING_HASH_FUNCTION,
                () -> RowContextCollectorExpression.objToBytesRef(r -> IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME))
            .put(InformationTablesTableInfo.Columns.CLOSED,
                () -> RowContextCollectorExpression.forFunction(PartitionInfo::isClosed))
            .put(InformationTablesTableInfo.Columns.TABLE_VERSION, PartitionsVersionExpression::new)
            .put(InformationTablesTableInfo.Columns.TABLE_SETTINGS, PartitionsSettingsExpression::new)
            .build();
    }

    private static ColumnRegistrar createColumnRegistrar() {
        return new ColumnRegistrar(IDENT, RowGranularity.DOC)
            .register(Columns.TABLE_SCHEMA, DataTypes.STRING)
            .register(InformationTablesTableInfo.Columns.TABLE_NAME, DataTypes.STRING)
            .register(Columns.PARTITION_IDENT, DataTypes.STRING)
            .register(Columns.VALUES, DataTypes.OBJECT)
            .register(InformationTablesTableInfo.Columns.NUMBER_OF_SHARDS, DataTypes.INTEGER)
            .register(InformationTablesTableInfo.Columns.NUMBER_OF_REPLICAS, DataTypes.STRING)
            .register(InformationTablesTableInfo.Columns.ROUTING_HASH_FUNCTION, DataTypes.STRING)
            .register(InformationTablesTableInfo.Columns.TABLE_VERSION, DataTypes.OBJECT)
            .register(InformationTablesTableInfo.Columns.TABLE_VERSION_CREATED, DataTypes.OBJECT)
            .register(InformationTablesTableInfo.Columns.TABLE_VERSION_CREATED_CRATEDB, DataTypes.STRING)
            .register(InformationTablesTableInfo.Columns.TABLE_VERSION_CREATED_ES, DataTypes.STRING)
            .register(InformationTablesTableInfo.Columns.TABLE_VERSION_UPGRADED, DataTypes.OBJECT)
            .register(InformationTablesTableInfo.Columns.TABLE_VERSION_UPGRADED_CRATEDB, DataTypes.STRING)
            .register(InformationTablesTableInfo.Columns.TABLE_VERSION_UPGRADED_ES, DataTypes.STRING)
            .register(InformationTablesTableInfo.Columns.CLOSED, DataTypes.BOOLEAN)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS, DataTypes.OBJECT)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_BLOCKS, DataTypes.OBJECT)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_BLOCKS_READ_ONLY, DataTypes.BOOLEAN)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_BLOCKS_READ, DataTypes.BOOLEAN)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_BLOCKS_WRITE, DataTypes.BOOLEAN)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_BLOCKS_METADATA, DataTypes.BOOLEAN)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_TRANSLOG, DataTypes.OBJECT)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_TRANSLOG_FLUSH_THRESHOLD_SIZE, DataTypes.LONG)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_TRANSLOG_SYNC_INTERVAL, DataTypes.LONG)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_ROUTING, DataTypes.OBJECT)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_ROUTING_ALLOCATION, DataTypes.OBJECT)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_ENABLE, DataTypes.STRING)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE, DataTypes.INTEGER)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_WARMER, DataTypes.OBJECT)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_WARMER_ENABLED, DataTypes.BOOLEAN)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_UNASSIGNED, DataTypes.OBJECT)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT, DataTypes.OBJECT)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT, DataTypes.LONG)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_MAPPING, DataTypes.OBJECT)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS, DataTypes.OBJECT)
            .register(InformationTablesTableInfo.Columns.TABLE_SETTINGS_MAPPING_TOTAL_FIELDS_LIMIT, DataTypes.INTEGER);
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
