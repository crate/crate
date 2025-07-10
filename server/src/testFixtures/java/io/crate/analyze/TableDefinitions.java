/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import static java.util.Collections.singletonList;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;

public final class TableDefinitions {

    private TableDefinitions() {}

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
    public static final String USER_TABLE_MULTI_PK_DEFINITION =
        "create table users_multi_pk (" +
        "  id bigint," +
        "  name text," +
        "  details object," +
        "  awesome boolean," +
        "  friends array(object)," +
        "  primary key (id, name)" +
        ")" +
        " clustered by (id)";
    public static final String USER_TABLE_CLUSTERED_BY_ONLY_DEFINITION =
        "create table doc.users_clustered_by_only (" +
        "  id bigint," +
        "  name text," +
        "  details object," +
        "  awesome boolean," +
        "  friends array(object)" +
        ")" +
        " clustered by (id)";
    static final RelationName TEST_PARTITIONED_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "parted");

    public static final String TEST_PARTITIONED_TABLE_DEFINITION =
        "create table doc.parted (" +
        "  id int," +
        "  name text," +
        "  date timestamp with time zone," +
        "  obj object" +
        ")" +
        " partitioned by (date)";
    public static final String[] TEST_PARTITIONED_TABLE_PARTITIONS = new String[] {
        new PartitionName(new RelationName("doc", "parted"), singletonList("1395874800000")).asIndexName(),
        new PartitionName(new RelationName("doc", "parted"), singletonList("1395961200000")).asIndexName(),
        new PartitionName(new RelationName("doc", "parted"), singletonList(null)).asIndexName()
    };

    public static final String PARTED_PKS_TABLE_DEFINITION =
        "create table doc.parted_pks (" +
        "  id integer," +
        "  name text," +
        "  date timestamp with time zone," +
        "  obj object," +
        "  primary key(id, date)" +
        ")" +
        " clustered by (id)" +
        " partitioned by (date)";
    public static final String TEST_DOC_TRANSACTIONS_TABLE_DEFINITION =
        "create table doc.transactions (" +
        "  id bigint," +
        "  sender text," +
        "  recipient text," +
        "  amount double precision," +
        "  timestamp timestamp with time zone" +
        ")";
    public static final String DEEPLY_NESTED_TABLE_DEFINITION =
        "create table doc.deeply_nested (" +
        "  details object as (" +
        "    awesome boolean," +
        "    stuff object as (" +
        "      name text" +
        "    )," +
        "    arguments array(object as (" +
        "      quality double precision," +
        "      name text" +
        "    ))" +
        "  )," +
        "  tags array(object as (" +
        "    name text," +
        "    metadata array(object as (" +
        "      id bigint" +
        "    ))" +
        "  ))" +
        ")";

    public static final RelationName TEST_DOC_LOCATIONS_TABLE_IDENT = new RelationName(Schemas.DOC_SCHEMA_NAME, "locations");

    public static final String TEST_DOC_LOCATIONS_TABLE_DEFINITION =
        "create table doc.locations (" +
        "  id bigint," +
        "  loc geo_point" +
        ")";
}
