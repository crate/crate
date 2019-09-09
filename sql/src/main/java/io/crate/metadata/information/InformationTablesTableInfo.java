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

import io.crate.common.collections.Lists2;
import io.crate.expression.reference.information.TablesSettingsExpression;
import io.crate.expression.reference.information.TablesVersionExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexMappings;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.ShardedTable;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ObjectType;
import org.elasticsearch.Version;

import java.util.List;
import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;

public class InformationTablesTableInfo extends InformationTableInfo<RelationInfo> {

    public static final String NAME = "tables";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    private static final String SELF_REFERENCING_COLUMN_NAME = "_id";
    private static final String REFERENCE_GENERATION = "SYSTEM GENERATED";

    private static ColumnRegistrar<RelationInfo> columnRegistrar() {
        return new ColumnRegistrar<RelationInfo>(IDENT, RowGranularity.DOC)
            .register("table_schema", STRING, () -> forFunction(r -> r.ident().schema()))
            .register("table_name", STRING, () -> forFunction(r -> r.ident().name()))
            .register("table_catalog", STRING, () -> forFunction(r -> r.ident().schema()))
            .register("table_type", STRING, () -> forFunction(r -> r.relationType().pretty()))
            .register("number_of_shards", INTEGER, () -> forFunction(row -> {
                if (row instanceof ShardedTable) {
                    return ((ShardedTable) row).numberOfShards();
                }
                return null;
            }))
            .register("number_of_replicas", STRING,
                () -> forFunction(row -> {
                    if (row instanceof ShardedTable) {
                        return ((ShardedTable) row).numberOfReplicas();
                    }
                    return null;
                }))
            .register("clustered_by", STRING,
                () -> forFunction(row -> {
                    if (row instanceof ShardedTable) {
                        ColumnIdent clusteredBy = ((ShardedTable) row).clusteredBy();
                        if (clusteredBy == null) {
                            return null;
                        }
                        return clusteredBy.fqn();
                    }
                    return null;
                }))
            .register("partitioned_by", STRING_ARRAY,
                () -> forFunction(row -> {
                    if (row instanceof DocTableInfo) {
                        List<ColumnIdent> partitionedBy = ((DocTableInfo) row).partitionedBy();
                        if (partitionedBy == null || partitionedBy.isEmpty()) {
                            return null;
                        }
                        return Lists2.map(partitionedBy, ColumnIdent::fqn);
                    }
                    return null;
                }))
            .register("blobs_path", STRING,
                () -> forFunction(row -> {
                    if (row instanceof BlobTableInfo) {
                        return ((BlobTableInfo) row).blobsPath();
                    }
                    return null;
                }))
            .register("column_policy", STRING,
                () -> forFunction(row -> {
                    if (row instanceof DocTableInfo) {
                        return ((DocTableInfo) row).columnPolicy().lowerCaseName();
                    }
                    return ColumnPolicy.STRICT.lowerCaseName();
                }))
            .register("routing_hash_function", STRING,
                () -> forFunction(row -> {
                    if (row instanceof ShardedTable) {
                        return IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME;
                    }
                    return null;
                }))
            .register("version", ObjectType.builder()
                .setInnerType(Version.Property.CREATED.toString(), STRING)
                .setInnerType(Version.Property.UPGRADED.toString(), STRING)
                .build(), TablesVersionExpression::new)
            .register("closed", BOOLEAN, () -> forFunction(row -> {
                if (row instanceof ShardedTable) {
                    return ((ShardedTable) row).isClosed();
                }
                return null;
            }))
            .register("reference_generation", STRING, () -> forFunction(r -> REFERENCE_GENERATION))
            .register("self_referencing_column_name", STRING,() -> forFunction(row -> {
                if (row instanceof ShardedTable) {
                    return SELF_REFERENCING_COLUMN_NAME;
                }
                return null;
            }))
            .register("settings", ObjectType.builder()
                .setInnerType("refresh_interval", LONG)
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
                        .setInnerType("require", ObjectType.untyped())
                        .setInnerType("include", ObjectType.untyped())
                        .setInnerType("exclude", ObjectType.untyped())
                        .build())
                    .build())
                .setInnerType("warmer", ObjectType.builder()
                    .setInnerType("enabled", BOOLEAN)
                    .build())
                .setInnerType("write", ObjectType.builder()
                    .setInnerType("wait_for_active_shards", STRING)
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
                .build(), TablesSettingsExpression::new);
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<RelationInfo>> expressions() {
        return columnRegistrar().expressions();
    }

    InformationTablesTableInfo() {
        super(IDENT, columnRegistrar(), "table_catalog", "table_schema", "table_name");
    }
}
