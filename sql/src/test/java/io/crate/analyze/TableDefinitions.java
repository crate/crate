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

import com.google.common.collect.ImmutableMap;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class TableDefinitions {

    public static final Routing SHARD_ROUTING = shardRouting("t1");

    private static final Routing PARTED_ROUTING = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
        .put("nodeOne", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(".partitioned.parted.04232chj", Arrays.asList(1, 2)).map())
        .put("nodeTwo", TreeMapBuilder.<String, List<Integer>>newMapBuilder().map())
        .map());


    private static final Routing CLUSTERED_PARTED_ROUTING = new Routing(
        TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
            .put("nodeOne", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(".partitioned.clustered_parted.04732cpp6ks3ed1o60o30c1g", Arrays.asList(1, 2)).map())
            .put("nodeTwo", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(".partitioned.clustered_parted.04732cpp6ksjcc9i60o30c1g", Arrays.asList(3)).map())
            .map());

    public static Routing shardRouting(String tableName) {
        return new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
            .put("nodeOne", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(tableName, Arrays.asList(1, 2)).map())
            .put("nodeTwo", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(tableName, Arrays.asList(3, 4)).map())
            .map());
    }

    public static Routing shardRoutingForReplicas(String tableName) {
        return new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
            .put("nodeOne", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(tableName, Arrays.asList(3, 4)).map())
            .put("nodeTwo", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(tableName, Arrays.asList(1, 2)).map())
            .map());
    }

    public static final TableIdent USER_TABLE_IDENT = new TableIdent(Schemas.DOC_SCHEMA_NAME, "users");

    public static final DocTableInfo USER_TABLE_INFO = TestingTableInfo.builder(USER_TABLE_IDENT, SHARD_ROUTING)
        .add("id", DataTypes.LONG, null)
        .add("other_id", DataTypes.LONG, null)
        .add("name", DataTypes.STRING, null)
        .add("text", DataTypes.STRING, null, Reference.IndexType.ANALYZED)
        .add("no_index", DataTypes.STRING, null, Reference.IndexType.NO)
        .add("details", DataTypes.OBJECT, null)
        .add("address", DataTypes.OBJECT, null, ColumnPolicy.STRICT)
        .add("postcode", DataTypes.STRING, Collections.singletonList("address"))
        .add("awesome", DataTypes.BOOLEAN, null)
        .add("counters", new ArrayType(DataTypes.LONG), null)
        .add("friends", new ArrayType(DataTypes.OBJECT), null, ColumnPolicy.DYNAMIC)
        .add("friends", DataTypes.LONG, Arrays.asList("id"))
        .add("friends", new ArrayType(DataTypes.STRING), Arrays.asList("groups"))
        .add("tags", new ArrayType(DataTypes.STRING), null)
        .add("bytes", DataTypes.BYTE, null)
        .add("shorts", DataTypes.SHORT, null)
        .add("date", DataTypes.TIMESTAMP, null)
        .add("shape", DataTypes.GEO_SHAPE)
        .add("ints", DataTypes.INTEGER, null)
        .add("floats", DataTypes.FLOAT, null)
        .addIndex(ColumnIdent.fromPath("name_text_ft"), Reference.IndexType.ANALYZED)
        .addPrimaryKey("id")
        .clusteredBy("id")
        .build();
    public static final TableIdent USER_TABLE_IDENT_MULTI_PK = new TableIdent(Schemas.DOC_SCHEMA_NAME, "users_multi_pk");
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
    public static final TableIdent USER_TABLE_IDENT_CLUSTERED_BY_ONLY = new TableIdent(Schemas.DOC_SCHEMA_NAME, "users_clustered_by_only");
    public static final DocTableInfo USER_TABLE_INFO_CLUSTERED_BY_ONLY = TestingTableInfo.builder(USER_TABLE_IDENT_CLUSTERED_BY_ONLY, SHARD_ROUTING)
        .add("id", DataTypes.LONG, null)
        .add("name", DataTypes.STRING, null)
        .add("details", DataTypes.OBJECT, null)
        .add("awesome", DataTypes.BOOLEAN, null)
        .add("friends", new ArrayType(DataTypes.OBJECT), null, ColumnPolicy.DYNAMIC)
        .clusteredBy("id")
        .build();
    static final TableIdent USER_TABLE_REFRESH_INTERVAL_BY_ONLY = new TableIdent(Schemas.DOC_SCHEMA_NAME, "user_refresh_interval");
    public static final DocTableInfo USER_TABLE_INFO_REFRESH_INTERVAL_BY_ONLY = TestingTableInfo.builder(USER_TABLE_REFRESH_INTERVAL_BY_ONLY, SHARD_ROUTING)
        .add("id", DataTypes.LONG, null)
        .add("content", DataTypes.STRING, null)
        .clusteredBy("id")
        .build();
    static final TableIdent NESTED_PK_TABLE_IDENT = new TableIdent(Schemas.DOC_SCHEMA_NAME, "nested_pk");
    public static final DocTableInfo NESTED_PK_TABLE_INFO = TestingTableInfo.builder(NESTED_PK_TABLE_IDENT, SHARD_ROUTING)
        .add("id", DataTypes.LONG, null)
        .add("o", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
        .add("o", DataTypes.BYTE, Arrays.asList("b"))
        .addPrimaryKey("id")
        .addPrimaryKey("o.b")
        .clusteredBy("o.b")
        .build();
    public static final TableIdent TEST_PARTITIONED_TABLE_IDENT = new TableIdent(Schemas.DOC_SCHEMA_NAME, "parted");
    public static final DocTableInfo TEST_PARTITIONED_TABLE_INFO = new TestingTableInfo.Builder(
        TEST_PARTITIONED_TABLE_IDENT, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
        .add("id", DataTypes.INTEGER, null)
        .add("name", DataTypes.STRING, null)
        .add("date", DataTypes.TIMESTAMP, null, true)
        .add("obj", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
        // add 3 partitions/simulate already done inserts
        .addPartitions(
            new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).asIndexName(),
            new PartitionName("parted", Arrays.asList(new BytesRef("1395961200000"))).asIndexName(),
            new PartitionName("parted", new ArrayList<BytesRef>() {{
                add(null);
            }}).asIndexName())
        .build();
    public static final TableIdent TEST_EMPTY_PARTITIONED_TABLE_IDENT =
        new TableIdent(Schemas.DOC_SCHEMA_NAME, "empty_parted");
    public static final DocTableInfo TEST_EMPTY_PARTITIONED_TABLE_INFO = new TestingTableInfo.Builder(
        TEST_EMPTY_PARTITIONED_TABLE_IDENT, new Routing(ImmutableMap.of()))
        .add("name", DataTypes.STRING, null)
        .add("date", DataTypes.TIMESTAMP, null, true)
        .build();
    public static final TableIdent PARTED_PKS_IDENT = new TableIdent(Schemas.DOC_SCHEMA_NAME, "parted_pks");
    public static final DocTableInfo PARTED_PKS_TI = new TestingTableInfo.Builder(
        PARTED_PKS_IDENT, PARTED_ROUTING)
        .add("id", DataTypes.INTEGER, null)
        .add("name", DataTypes.STRING, null)
        .add("date", DataTypes.TIMESTAMP, null, true)
        .add("obj", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
        // add 3 partitions/simulate already done inserts
        .addPartitions(
            new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).asIndexName(),
            new PartitionName("parted", Arrays.asList(new BytesRef("1395961200000"))).asIndexName()
        )
        .addPrimaryKey("id")
        .addPrimaryKey("date")
        .clusteredBy("id")
        .build();
    public static final TableIdent TEST_MULTIPLE_PARTITIONED_TABLE_IDENT = new TableIdent(Schemas.DOC_SCHEMA_NAME, "multi_parted");
    public static final DocTableInfo TEST_MULTIPLE_PARTITIONED_TABLE_INFO = new TestingTableInfo.Builder(
        TEST_MULTIPLE_PARTITIONED_TABLE_IDENT, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
        .add("id", DataTypes.INTEGER, null)
        .add("date", DataTypes.TIMESTAMP, null, true)
        .add("num", DataTypes.LONG, null)
        .add("obj", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
        .add("obj", DataTypes.STRING, Arrays.asList("name"), true)
        // add 3 partitions/simulate already done inserts
        .addPartitions(
            new PartitionName("multi_parted", Arrays.asList(new BytesRef("1395874800000"), new BytesRef("0"))).asIndexName(),
            new PartitionName("multi_parted", Arrays.asList(new BytesRef("1395961200000"), new BytesRef("-100"))).asIndexName(),
            new PartitionName("multi_parted", Arrays.asList(null, new BytesRef("-100"))).asIndexName())
        .build();
    static final TableIdent TEST_NESTED_PARTITIONED_TABLE_IDENT = new TableIdent(Schemas.DOC_SCHEMA_NAME, "nested_parted");
    public static final DocTableInfo TEST_NESTED_PARTITIONED_TABLE_INFO = new TestingTableInfo.Builder(
        TEST_NESTED_PARTITIONED_TABLE_IDENT, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
        .add("id", DataTypes.INTEGER, null)
        .add("date", DataTypes.TIMESTAMP, null, true)
        .add("obj", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
        .add("obj", DataTypes.STRING, Arrays.asList("name"), true)
        // add 3 partitions/simulate already done inserts
        .addPartitions(
            new PartitionName("nested_parted", Arrays.asList(new BytesRef("1395874800000"), new BytesRef("Trillian"))).asIndexName(),
            new PartitionName("nested_parted", Arrays.asList(new BytesRef("1395961200000"), new BytesRef("Ford"))).asIndexName(),
            new PartitionName("nested_parted", Arrays.asList(null, new BytesRef("Zaphod"))).asIndexName())
        .build();
    public static final TableIdent TEST_DOC_TRANSACTIONS_TABLE_IDENT = new TableIdent(Schemas.DOC_SCHEMA_NAME, "transactions");
    public static final DocTableInfo TEST_DOC_TRANSACTIONS_TABLE_INFO = new TestingTableInfo.Builder(
        TEST_DOC_TRANSACTIONS_TABLE_IDENT, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
        .add("id", DataTypes.LONG, null)
        .add("sender", DataTypes.STRING, null)
        .add("recipient", DataTypes.STRING, null)
        .add("amount", DataTypes.DOUBLE, null)
        .add("timestamp", DataTypes.TIMESTAMP, null)
        .build();
    public static final TableIdent DEEPLY_NESTED_TABLE_IDENT = new TableIdent(Schemas.DOC_SCHEMA_NAME, "deeply_nested");
    public static final DocTableInfo DEEPLY_NESTED_TABLE_INFO = new TestingTableInfo.Builder(
        DEEPLY_NESTED_TABLE_IDENT, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
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

    public static final TableIdent IGNORED_NESTED_TABLE_IDENT = new TableIdent(Schemas.DOC_SCHEMA_NAME, "ignored_nested");
    public static final DocTableInfo IGNORED_NESTED_TABLE_INFO = new TestingTableInfo.Builder(
        IGNORED_NESTED_TABLE_IDENT, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
        .add("details", DataTypes.OBJECT, null, ColumnPolicy.IGNORED)
        .build();

    public static final TableIdent TEST_DOC_LOCATIONS_TABLE_IDENT = new TableIdent(Schemas.DOC_SCHEMA_NAME, "locations");
    public static final DocTableInfo TEST_DOC_LOCATIONS_TABLE_INFO = TestingTableInfo.builder(TEST_DOC_LOCATIONS_TABLE_IDENT, SHARD_ROUTING)
        .add("id", DataTypes.LONG, null)
        .add("loc", DataTypes.GEO_POINT, null)
        .build();

    public static final DocTableInfo TEST_CLUSTER_BY_STRING_TABLE_INFO = TestingTableInfo.builder(new TableIdent(Schemas.DOC_SCHEMA_NAME, "bystring"), SHARD_ROUTING)
        .add("name", DataTypes.STRING, null)
        .add("score", DataTypes.DOUBLE, null)
        .addPrimaryKey("name")
        .clusteredBy("name")
        .build();


    public static final TableIdent CLUSTERED_PARTED_IDENT = new TableIdent(Schemas.DOC_SCHEMA_NAME, "clustered_parted");
    public static final DocTableInfo CLUSTERED_PARTED = new TestingTableInfo.Builder(
        CLUSTERED_PARTED_IDENT, CLUSTERED_PARTED_ROUTING)
        .add("id", DataTypes.INTEGER, null)
        .add("date", DataTypes.TIMESTAMP, null, true)
                .add("city", DataTypes.STRING, null)
                .clusteredBy("city")
                .addPartitions(
                    new PartitionName("clustered_parted", Arrays.asList(new BytesRef("1395874800000"))).asIndexName(),
                    new PartitionName("clustered_parted", Arrays.asList(new BytesRef("1395961200000"))).asIndexName())
                .build();


    public static TestingBlobTableInfo createBlobTable(TableIdent ident, ClusterService clusterService) {
        return new TestingBlobTableInfo(
            ident,
            ident.indexName(),
            clusterService,
            5,
            new BytesRef("0"),
            ImmutableMap.<String, Object>of(),
            null,
            SHARD_ROUTING
        );
    }
}
