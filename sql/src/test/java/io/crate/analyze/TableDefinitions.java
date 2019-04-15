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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIndexedContainer;
import com.google.common.collect.ImmutableMap;
import io.crate.common.collections.TreeMapBuilder;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

import java.util.Map;

import static java.util.Collections.singletonList;

public final class TableDefinitions {

    static final Routing SHARD_ROUTING = shardRouting("t1");

    private static final Routing PARTED_ROUTING = new Routing(TreeMapBuilder.<String, Map<String, IntIndexedContainer>>newMapBuilder()
        .put("n1", TreeMapBuilder.<String, IntIndexedContainer>newMapBuilder().put(".partitioned.parted.04232chj", IntArrayList.from(1, 2)).map())
        .put("n2", TreeMapBuilder.<String, IntIndexedContainer>newMapBuilder().map())
        .map());

    public static Routing shardRouting(String tableName) {
        return new Routing(TreeMapBuilder.<String, Map<String, IntIndexedContainer>>newMapBuilder()
            .put("n1", TreeMapBuilder.<String, IntIndexedContainer>newMapBuilder().put(tableName, IntArrayList.from(1, 2)).map())
            .put("n2", TreeMapBuilder.<String, IntIndexedContainer>newMapBuilder().put(tableName, IntArrayList.from(3, 4)).map())
            .map());
    }

    public static final RelationName USER_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "users");

    public static final String USER_TABLE_DEFINITION =
        "create table doc.users (" +
         "  id long primary key," +
         "  other_id bigint," +
         "  name string," +
         "  text string index using fulltext," +
         "  no_index string index off," +
         "  details object," +
         "  address object (strict) as (" +
         "      postcode string" +
         "  )," +
         "  awesome boolean," +
         "  counters array(long)," +
         "  friends array(object as (" +
         "      id long," +
         "      groups array(string)" +
         "  ))," +
         "  tags array(string)," +
         "  bytes byte," +
         "  shorts short," +
         "  date timestamp with time zone," +
         "  shape geo_shape," +
         "  ints integer," +
         "  floats float," +
         "  index name_text_ft using fulltext (name, text)" +
         ") clustered by (id) with (column_policy = 'dynamic')";
    private static final RelationName USER_TABLE_IDENT_MULTI_PK = new RelationName(Schemas.DOC_SCHEMA_NAME, "users_multi_pk");
    public static final DocTableInfo USER_TABLE_INFO_MULTI_PK = TestingTableInfo.builder(USER_TABLE_IDENT_MULTI_PK, SHARD_ROUTING)
        .add("id", DataTypes.LONG)
        .add("name", DataTypes.STRING)
        .add("details", ObjectType.untyped())
        .add("awesome", DataTypes.BOOLEAN)
        .add("friends", new ArrayType(ObjectType.untyped()))
        .addPrimaryKey("id")
        .addPrimaryKey("name")
        .clusteredBy("id")
        .build();
    private static final RelationName USER_TABLE_IDENT_CLUSTERED_BY_ONLY = new RelationName(Schemas.DOC_SCHEMA_NAME, "users_clustered_by_only");
    public static final DocTableInfo USER_TABLE_INFO_CLUSTERED_BY_ONLY = TestingTableInfo.builder(USER_TABLE_IDENT_CLUSTERED_BY_ONLY, SHARD_ROUTING)
        .add("id", DataTypes.LONG)
        .add("name", DataTypes.STRING)
        .add("details", ObjectType.untyped())
        .add("awesome", DataTypes.BOOLEAN)
        .add("friends", new ArrayType(ObjectType.untyped()))
        .clusteredBy("id")
        .build();
    private static final RelationName USER_TABLE_REFRESH_INTERVAL_BY_ONLY = new RelationName(Schemas.DOC_SCHEMA_NAME, "user_refresh_interval");
    public static final DocTableInfo USER_TABLE_INFO_REFRESH_INTERVAL_BY_ONLY = TestingTableInfo.builder(USER_TABLE_REFRESH_INTERVAL_BY_ONLY, SHARD_ROUTING)
        .add("id", DataTypes.LONG)
        .add("content", DataTypes.STRING)
        .clusteredBy("id")
        .build();
    private static final RelationName NESTED_PK_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "nested_pk");
    public static final DocTableInfo NESTED_PK_TABLE_INFO = TestingTableInfo.builder(NESTED_PK_TABLE_IDENT, SHARD_ROUTING)
        .add("id", DataTypes.LONG)
        .add("o",
            ObjectType.builder()
                .setInnerType("b", DataTypes.BYTE)
                .build())
        .addPrimaryKey("id")
        .addPrimaryKey("o.b")
        .clusteredBy("o.b")
        .build();
    static final RelationName TEST_PARTITIONED_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "parted");

    public static final DocTableInfo TEST_PARTITIONED_TABLE_INFO = new TestingTableInfo.Builder(
        TEST_PARTITIONED_TABLE_IDENT, new Routing(ImmutableMap.of()))
        .add("id", DataTypes.INTEGER)
        .add("name", DataTypes.STRING)
        .add("date", DataTypes.TIMESTAMPZ, null, true)
        .add("obj", ObjectType.untyped())
        // add 3 partitions/simulate already done inserts
        .addPartitions(
            new PartitionName(new RelationName("doc", "parted"), singletonList("1395874800000")).asIndexName(),
            new PartitionName(new RelationName("doc", "parted"), singletonList("1395961200000")).asIndexName(),
            new PartitionName(new RelationName("doc", "parted"), singletonList(null)).asIndexName())
        .build();
    private static final RelationName TEST_EMPTY_PARTITIONED_TABLE_IDENT =
        new RelationName(Schemas.DOC_SCHEMA_NAME, "empty_parted");
    public static final DocTableInfo TEST_EMPTY_PARTITIONED_TABLE_INFO = new TestingTableInfo.Builder(
        TEST_EMPTY_PARTITIONED_TABLE_IDENT, new Routing(ImmutableMap.of()))
        .add("name", DataTypes.STRING)
        .add("date", DataTypes.TIMESTAMPZ, null, true)
        .build();
    public static final RelationName PARTED_PKS_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "parted_pks");
    public static final DocTableInfo PARTED_PKS_TI = new TestingTableInfo.Builder(
        PARTED_PKS_IDENT, PARTED_ROUTING)
        .add("id", DataTypes.INTEGER)
        .add("name", DataTypes.STRING)
        .add("date", DataTypes.TIMESTAMPZ, null, true)
        .add("obj", ObjectType.untyped())
        // add 3 partitions/simulate already done inserts
        .addPartitions(
            new PartitionName(new RelationName("doc", "parted"), singletonList("1395874800000")).asIndexName(),
            new PartitionName(new RelationName("doc", "parted"), singletonList("1395961200000")).asIndexName()
        )
        .addPrimaryKey("id")
        .addPrimaryKey("date")
        .clusteredBy("id")
        .build();
    private static final RelationName TEST_DOC_TRANSACTIONS_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "transactions");
    public static final DocTableInfo TEST_DOC_TRANSACTIONS_TABLE_INFO = new TestingTableInfo.Builder(
        TEST_DOC_TRANSACTIONS_TABLE_IDENT, new Routing(ImmutableMap.of()))
        .add("id", DataTypes.LONG)
        .add("sender", DataTypes.STRING)
        .add("recipient", DataTypes.STRING)
        .add("amount", DataTypes.DOUBLE)
        .add("timestamp", DataTypes.TIMESTAMPZ)
        .build();
    private static final RelationName DEEPLY_NESTED_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "deeply_nested");
    public static final DocTableInfo DEEPLY_NESTED_TABLE_INFO = new TestingTableInfo.Builder(
        DEEPLY_NESTED_TABLE_IDENT, new Routing(ImmutableMap.of()))
        .add("details",
            ObjectType.builder()
                .setInnerType("awesome", DataTypes.BOOLEAN)
                .setInnerType("stuff", ObjectType.builder()
                    .setInnerType("name", DataTypes.STRING)
                    .build())
                .setInnerType("arguments", new ArrayType(ObjectType.builder()
                    .setInnerType("quality", DataTypes.DOUBLE)
                    .setInnerType("name", DataTypes.STRING)
                    .build()))
                .build())
        .add("tags", new ArrayType(ObjectType.builder()
            .setInnerType("name", DataTypes.STRING)
            .setInnerType("metadata", new ArrayType(ObjectType.builder()
                .setInnerType("id", DataTypes.LONG)
                .build()))
            .build()))
        .build();

    private static final RelationName IGNORED_NESTED_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "ignored_nested");
    public static final DocTableInfo IGNORED_NESTED_TABLE_INFO = new TestingTableInfo.Builder(
        IGNORED_NESTED_TABLE_IDENT, new Routing(ImmutableMap.of()))
        .add("details", ObjectType.untyped(), null, ColumnPolicy.IGNORED)
        .build();

    public static final RelationName TEST_DOC_LOCATIONS_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "locations");
    public static final DocTableInfo TEST_DOC_LOCATIONS_TABLE_INFO = TestingTableInfo.builder(TEST_DOC_LOCATIONS_TABLE_IDENT, SHARD_ROUTING)
        .add("id", DataTypes.LONG)
        .add("loc", DataTypes.GEO_POINT)
        .build();

    public static final DocTableInfo TEST_CLUSTER_BY_STRING_TABLE_INFO = TestingTableInfo.builder(new RelationName(Schemas.DOC_SCHEMA_NAME, "bystring"), SHARD_ROUTING)
        .add("name", DataTypes.STRING)
        .add("score", DataTypes.DOUBLE)
        .addPrimaryKey("name")
        .clusteredBy("name")
        .build();


    public static TestingBlobTableInfo createBlobTable(RelationName ident) {
        return new TestingBlobTableInfo(
            ident,
            ident.indexNameOrAlias(),
            5,
            "0",
            ImmutableMap.of(),
            null,
            SHARD_ROUTING
        );
    }
}
