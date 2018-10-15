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
import io.crate.core.collections.TreeMapBuilder;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public final class TableDefinitions {

    public static final Routing SHARD_ROUTING = shardRouting("t1");

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
         "  date timestamp," +
         "  shape geo_shape," +
         "  ints integer," +
         "  floats float," +
         "  index name_text_ft using fulltext (name, text)" +
         ") clustered by (id)";
    public static final RelationName USER_TABLE_IDENT_MULTI_PK = new RelationName(Schemas.DOC_SCHEMA_NAME, "users_multi_pk");
    public static final DocTableInfo USER_TABLE_INFO_MULTI_PK = TestingTableInfo.builder(USER_TABLE_IDENT_MULTI_PK, SHARD_ROUTING)
        .add("id", DataTypes.LONG, null)
        .add("name", DataTypes.STRING, null)
        .add("details", DataTypes.OBJECT, null)
        .add("awesome", DataTypes.BOOLEAN, null)
        .add("friends", new ArrayType(DataTypes.OBJECT), null, ColumnPolicy.DYNAMIC)
        .addPrimaryKey("id")
        .addPrimaryKey("name")
        .clusteredBy("id")
        .build();
    public static final RelationName USER_TABLE_IDENT_CLUSTERED_BY_ONLY = new RelationName(Schemas.DOC_SCHEMA_NAME, "users_clustered_by_only");
    public static final DocTableInfo USER_TABLE_INFO_CLUSTERED_BY_ONLY = TestingTableInfo.builder(USER_TABLE_IDENT_CLUSTERED_BY_ONLY, SHARD_ROUTING)
        .add("id", DataTypes.LONG, null)
        .add("name", DataTypes.STRING, null)
        .add("details", DataTypes.OBJECT, null)
        .add("awesome", DataTypes.BOOLEAN, null)
        .add("friends", new ArrayType(DataTypes.OBJECT), null, ColumnPolicy.DYNAMIC)
        .clusteredBy("id")
        .build();
    static final RelationName USER_TABLE_REFRESH_INTERVAL_BY_ONLY = new RelationName(Schemas.DOC_SCHEMA_NAME, "user_refresh_interval");
    public static final DocTableInfo USER_TABLE_INFO_REFRESH_INTERVAL_BY_ONLY = TestingTableInfo.builder(USER_TABLE_REFRESH_INTERVAL_BY_ONLY, SHARD_ROUTING)
        .add("id", DataTypes.LONG, null)
        .add("content", DataTypes.STRING, null)
        .clusteredBy("id")
        .build();
    static final RelationName NESTED_PK_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "nested_pk");
    public static final DocTableInfo NESTED_PK_TABLE_INFO = TestingTableInfo.builder(NESTED_PK_TABLE_IDENT, SHARD_ROUTING)
        .add("id", DataTypes.LONG, null)
        .add("o", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
        .add("o", DataTypes.BYTE, Arrays.asList("b"))
        .addPrimaryKey("id")
        .addPrimaryKey("o.b")
        .clusteredBy("o.b")
        .build();
    public static final RelationName TEST_PARTITIONED_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "parted");

    public static final DocTableInfo TEST_PARTITIONED_TABLE_INFO = new TestingTableInfo.Builder(
        TEST_PARTITIONED_TABLE_IDENT, new Routing(ImmutableMap.of()))
        .add("id", DataTypes.INTEGER, null)
        .add("name", DataTypes.STRING, null)
        .add("date", DataTypes.TIMESTAMP, null, true)
        .add("obj", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
        // add 3 partitions/simulate already done inserts
        .addPartitions(
            new PartitionName(new RelationName("doc", "parted"), Arrays.asList(new BytesRef("1395874800000"))).asIndexName(),
            new PartitionName(new RelationName("doc", "parted"), Arrays.asList(new BytesRef("1395961200000"))).asIndexName(),
            new PartitionName(new RelationName("doc", "parted"), new ArrayList<BytesRef>() {{
                add(null);
            }}).asIndexName())
        .build();
    public static final RelationName TEST_EMPTY_PARTITIONED_TABLE_IDENT =
        new RelationName(Schemas.DOC_SCHEMA_NAME, "empty_parted");
    public static final DocTableInfo TEST_EMPTY_PARTITIONED_TABLE_INFO = new TestingTableInfo.Builder(
        TEST_EMPTY_PARTITIONED_TABLE_IDENT, new Routing(ImmutableMap.of()))
        .add("name", DataTypes.STRING, null)
        .add("date", DataTypes.TIMESTAMP, null, true)
        .build();
    public static final RelationName PARTED_PKS_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "parted_pks");
    public static final DocTableInfo PARTED_PKS_TI = new TestingTableInfo.Builder(
        PARTED_PKS_IDENT, PARTED_ROUTING)
        .add("id", DataTypes.INTEGER, null)
        .add("name", DataTypes.STRING, null)
        .add("date", DataTypes.TIMESTAMP, null, true)
        .add("obj", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
        // add 3 partitions/simulate already done inserts
        .addPartitions(
            new PartitionName(new RelationName("doc", "parted"), Arrays.asList(new BytesRef("1395874800000"))).asIndexName(),
            new PartitionName(new RelationName("doc", "parted"), Arrays.asList(new BytesRef("1395961200000"))).asIndexName()
        )
        .addPrimaryKey("id")
        .addPrimaryKey("date")
        .clusteredBy("id")
        .build();
    public static final RelationName TEST_MULTIPLE_PARTITIONED_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "multi_parted");
    public static final DocTableInfo TEST_MULTIPLE_PARTITIONED_TABLE_INFO = new TestingTableInfo.Builder(
        TEST_MULTIPLE_PARTITIONED_TABLE_IDENT, new Routing(ImmutableMap.of()))
        .add("id", DataTypes.INTEGER, null)
        .add("date", DataTypes.TIMESTAMP, null, true)
        .add("num", DataTypes.LONG, null)
        .add("obj", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
        .add("obj", DataTypes.STRING, Arrays.asList("name"), true)
        // add 3 partitions/simulate already done inserts
        .addPartitions(
            new PartitionName(new RelationName("doc", "multi_parted"), Arrays.asList(new BytesRef("1395874800000"), new BytesRef("0"))).asIndexName(),
            new PartitionName(new RelationName("doc", "multi_parted"), Arrays.asList(new BytesRef("1395961200000"), new BytesRef("-100"))).asIndexName(),
            new PartitionName(new RelationName("doc", "multi_parted"), Arrays.asList(null, new BytesRef("-100"))).asIndexName())
        .build();
    static final RelationName TEST_NESTED_PARTITIONED_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "nested_parted");
    public static final DocTableInfo TEST_NESTED_PARTITIONED_TABLE_INFO = new TestingTableInfo.Builder(
        TEST_NESTED_PARTITIONED_TABLE_IDENT, new Routing(ImmutableMap.of()))
        .add("id", DataTypes.INTEGER, null)
        .add("date", DataTypes.TIMESTAMP, null, true)
        .add("obj", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
        .add("obj", DataTypes.STRING, Arrays.asList("name"), true)
        // add 3 partitions/simulate already done inserts
        .addPartitions(
            new PartitionName(new RelationName("doc", "nested_parted"), Arrays.asList(new BytesRef("1395874800000"), new BytesRef("Trillian"))).asIndexName(),
            new PartitionName(new RelationName("doc", "nested_parted"), Arrays.asList(new BytesRef("1395961200000"), new BytesRef("Ford"))).asIndexName(),
            new PartitionName(new RelationName("doc", "nested_parted"), Arrays.asList(null, new BytesRef("Zaphod"))).asIndexName())
        .build();
    public static final RelationName TEST_DOC_TRANSACTIONS_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "transactions");
    public static final DocTableInfo TEST_DOC_TRANSACTIONS_TABLE_INFO = new TestingTableInfo.Builder(
        TEST_DOC_TRANSACTIONS_TABLE_IDENT, new Routing(ImmutableMap.of()))
        .add("id", DataTypes.LONG, null)
        .add("sender", DataTypes.STRING, null)
        .add("recipient", DataTypes.STRING, null)
        .add("amount", DataTypes.DOUBLE, null)
        .add("timestamp", DataTypes.TIMESTAMP, null)
        .build();
    public static final RelationName DEEPLY_NESTED_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "deeply_nested");
    public static final DocTableInfo DEEPLY_NESTED_TABLE_INFO = new TestingTableInfo.Builder(
        DEEPLY_NESTED_TABLE_IDENT, new Routing(ImmutableMap.of()))
        .add("details", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
        .add("details", DataTypes.BOOLEAN, Arrays.asList("awesome"))
        .add("details", DataTypes.OBJECT, Arrays.asList("stuff"), ColumnPolicy.DYNAMIC)
        .add("details", DataTypes.STRING, Arrays.asList("stuff", "name"))
        .add("details", new ArrayType(DataTypes.OBJECT), Arrays.asList("arguments"))
        .add("details", DataTypes.DOUBLE, Arrays.asList("arguments", "quality"))
        .add("details", DataTypes.STRING, Arrays.asList("arguments", "name"))
        .add("tags", new ArrayType(DataTypes.OBJECT), null)
        .add("tags", DataTypes.STRING, Arrays.asList("name"))
        .add("tags", new ArrayType(DataTypes.OBJECT), Arrays.asList("metadata"))
        .add("tags", DataTypes.LONG, Arrays.asList("metadata", "id"))
        .build();

    public static final RelationName IGNORED_NESTED_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "ignored_nested");
    public static final DocTableInfo IGNORED_NESTED_TABLE_INFO = new TestingTableInfo.Builder(
        IGNORED_NESTED_TABLE_IDENT, new Routing(ImmutableMap.of()))
        .add("details", DataTypes.OBJECT, null, ColumnPolicy.IGNORED)
        .build();

    public static final RelationName TEST_DOC_LOCATIONS_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "locations");
    public static final DocTableInfo TEST_DOC_LOCATIONS_TABLE_INFO = TestingTableInfo.builder(TEST_DOC_LOCATIONS_TABLE_IDENT, SHARD_ROUTING)
        .add("id", DataTypes.LONG, null)
        .add("loc", DataTypes.GEO_POINT, null)
        .build();

    public static final DocTableInfo TEST_CLUSTER_BY_STRING_TABLE_INFO = TestingTableInfo.builder(new RelationName(Schemas.DOC_SCHEMA_NAME, "bystring"), SHARD_ROUTING)
        .add("name", DataTypes.STRING, null)
        .add("score", DataTypes.DOUBLE, null)
        .addPrimaryKey("name")
        .clusteredBy("name")
        .build();




    public static TestingBlobTableInfo createBlobTable(RelationName ident) {
        return new TestingBlobTableInfo(
            ident,
            ident.indexName(),
            5,
            new BytesRef("0"),
            ImmutableMap.<String, Object>of(),
            null,
            SHARD_ROUTING
        );
    }
}
