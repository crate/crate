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
import io.crate.types.ObjectType;
import org.elasticsearch.Version;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.STRING;

public class InformationPartitionsTableInfo extends InformationTableInfo<PartitionInfo> {

    public static final String NAME = "table_partitions";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    public static Map<ColumnIdent, RowCollectExpressionFactory<PartitionInfo>> expressions() {
        return columnRegistrar().expressions();
    }

    private static ColumnRegistrar<PartitionInfo> columnRegistrar() {
        return new ColumnRegistrar<PartitionInfo>(IDENT, RowGranularity.DOC)
            .register("table_schema", STRING, () -> forFunction(r -> r.name().relationName().schema()))
            .register("table_name", STRING, () -> forFunction(r -> r.name().relationName().name()))
            .register("partition_ident", STRING, () -> forFunction(r -> r.name().ident()))
            .register("values", ObjectType.untyped(), () -> new NestableCollectExpression<PartitionInfo, Map<String, Object>>() {
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
                        PartitionInfo::values, Collections.singletonList(name), STRING::value);
                }
            })
            .register("number_of_shards", INTEGER, () -> forFunction(PartitionInfo::numberOfShards))
            .register("number_of_replicas", STRING, () -> forFunction(PartitionInfo::numberOfReplicas))
            .register("routing_hash_function", STRING, () -> forFunction(r -> IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME))
            .register("closed", BOOLEAN, () -> forFunction(PartitionInfo::isClosed))
            .register("version", ObjectType.builder()
                .setInnerType(Version.Property.CREATED.toString(), STRING)
                .setInnerType(Version.Property.UPGRADED.toString(), STRING)
                .build(), PartitionsVersionExpression::new)
            .register("settings", ObjectType.builder()
                .setInnerType("blocks", ObjectType.builder()
                    .setInnerType("read_only", BOOLEAN)
                    .setInnerType("read", BOOLEAN)
                    .setInnerType("write", BOOLEAN)
                    .setInnerType("metadata", BOOLEAN)
                    .build())
                .setInnerType("codec", STRING)
                .setInnerType("store", ObjectType.builder()
                    .setInnerType("type", STRING)
                    .build())
                .setInnerType("translog", ObjectType.builder()
                    .setInnerType("flush_threshold_size", LONG)
                    .setInnerType("sync_interval", LONG)
                    .build())
                .setInnerType("routing", ObjectType.builder()
                    .setInnerType("allocation", ObjectType.builder()
                        .setInnerType("enable", STRING)
                        .setInnerType("total_shards_per_node", INTEGER)
                        .build())
                    .build())
                .setInnerType("warmer", ObjectType.builder()
                    .setInnerType("enabled", BOOLEAN)
                    .build())
                .setInnerType("unassigned", ObjectType.builder()
                    .setInnerType("node_left", ObjectType.builder()
                        .setInnerType("delayed_timeout", LONG)
                        .build())
                    .build())
                .setInnerType("mapping", ObjectType.builder()
                    .setInnerType("total_fields", ObjectType.builder()
                        .setInnerType("limit", INTEGER)
                        .build())
                    .build())
                .setInnerType("write", ObjectType.builder()
                    .setInnerType("wait_for_active_shards", STRING).build())
                .build(), PartitionsSettingsExpression::new);
    }

    InformationPartitionsTableInfo() {
        super(IDENT, columnRegistrar(), "table_schema","table_name", "partition_ident");
    }

    @Nullable
    @Override
    public Reference getReference(ColumnIdent column) {
        if (!column.isTopLevel() && column.name().equals("values")) {
            DynamicReference ref = new DynamicReference(new ReferenceIdent(ident(), column), rowGranularity());
            ref.valueType(STRING);
            return ref;
        }
        return super.getReference(column);
    }
}
