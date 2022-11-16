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

package io.crate.execution.dml;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.elasticsearch.index.mapper.ParsedDocument;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public class IndexerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_index_object_with_dynamic_column_creation() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (o object as (x int))")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference o = table.getReference(new ColumnIdent("o"));
        Indexer indexer = new Indexer(table, List.of(o));

        Map<String, Object> value = Map.of("x", 10, "y", 20);
        ParsedDocument parsedDoc = indexer.index(new Object[] { value });
        assertThat(parsedDoc.doc().getFields())
            .hasSize(8);

        assertThat(parsedDoc.newColumns())
            .hasSize(1);

        assertThat(parsedDoc.source().utf8ToString()).isIn(
            "{\"o\":{\"x\":10,\"y\":20}}",
            "{\"o\":{\"y\":20,\"x\":10}}"
        );

        // TODO:
        //  - defaults
        //  - generated columns
        //  - constraint checks
    }

    @Test
    public void test_create_dynamic_object_with_nested_columns() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (o object as (x int))")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference o = table.getReference(new ColumnIdent("o"));
        Indexer indexer = new Indexer(table, List.of(o));

        Map<String, Object> value = Map.of("x", 10, "obj", Map.of("y", 20));
        ParsedDocument parsedDoc = indexer.index(new Object[] { value });
        assertThat(parsedDoc.doc().getFields())
            .hasSize(8);

        assertThat(parsedDoc.newColumns())
            .satisfiesExactly(
                col1 -> assertThat(col1).isReference("o['obj']"),
                col2 -> assertThat(col2).isReference("o['obj']['y']")
            );
    }

    @Test
    public void test_create_dynamic_array() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (o object as (x int))")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference o = table.getReference(new ColumnIdent("o"));
        Indexer indexer = new Indexer(table, List.of(o));

        Map<String, Object> value = Map.of("x", 10, "xs", List.of(2, 3, 4));
        ParsedDocument parsedDoc = indexer.index(new Object[] { value });
        assertThat(parsedDoc.doc().getFields())
            .hasSize(12);

        assertThat(parsedDoc.newColumns())
            .satisfiesExactly(
                col1 -> assertThat(col1).isReference("o['xs']", new ArrayType<>(DataTypes.INTEGER))
            );

        assertThat(parsedDoc.source().utf8ToString()).isIn(
            "{\"o\":{\"x\":10,\"xs\":[2,3,4]}}",
            "{\"o\":{\"xs\":[2,3,4],\"x\":10}}"
        );
    }
}
