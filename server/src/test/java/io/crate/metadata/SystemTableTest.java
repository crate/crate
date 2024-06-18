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

package io.crate.metadata;

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.isReference;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public class SystemTableTest {

    @Test
    public void test_create_system_table_with_nested_object() {
        var relation = new RelationName("doc", "dummy");
        var table = SystemTable.builder(relation)
            .startObject("obj_a")
                .startObject("obj_b")
                    .add("x", DataTypes.INTEGER, x -> 1)
                .endObject()
            .endObject()
            .build();

        assertThat(table.columns()).satisfiesExactly(isReference("obj_a"));
        assertThat(table.getReference(ColumnIdent.of("obj_a", List.of("obj_b", "x"))))
            .isReference().hasName("obj_a['obj_b']['x']");

        var x = table.expressions().get(ColumnIdent.of("obj_a", List.of("obj_b", "x"))).create();
        x.setNextRow(null);
        assertThat(x.value()).isEqualTo(1);

        var objB = table.expressions().get(ColumnIdent.of("obj_a", "obj_b")).create();
        objB.setNextRow(null);
        assertThat(objB.value()).isEqualTo(Map.of("x", 1));

        var objA = table.expressions().get(ColumnIdent.of("obj_a")).create();
        objA.setNextRow(null);
        System.out.println(objA.value());
        assertThat(objA.value()).isEqualTo(Map.of("obj_b", Map.of("x", 1)));
    }

    static class Point {

        private final int x;
        private final int y;

        Point(int x, int y) {
            this.x = x;
            this.y = y;
        }
    }

    @Test
    public void test_object_array() throws Exception {
        var relation = new RelationName("doc", "dummy");
        var table = SystemTable.builder(relation)
            .startObjectArray("points", x -> List.of(new Point(10, 20), new Point(30, 40)))
                .add("x", DataTypes.INTEGER, point -> point.x)
                .add("y", DataTypes.INTEGER, point -> point.y)
            .endObjectArray()
            .build();
        assertThat(table.getReference(ColumnIdent.of("points"))).isReference().hasName("points");
        var points = table.expressions().get(ColumnIdent.of("points")).create();
        points.setNextRow(null);
        assertThat(points.value()).isEqualTo(
            List.of(
                Map.of("x", 10, "y", 20),
                Map.of("x", 30, "y", 40)));
        assertThat(table.getReference(ColumnIdent.of("points", "x")))
            .isReference()
            .hasName("points['x']")
            .hasType(new ArrayType<>(DataTypes.INTEGER));
        var xs = table.expressions().get(ColumnIdent.of("points", "x")).create();
        xs.setNextRow(null);
        assertThat(xs.value()).isEqualTo(List.of(10, 30));

        var ys = table.expressions().get(ColumnIdent.of("points", "y")).create();
        ys.setNextRow(null);
        assertThat(ys.value()).isEqualTo(List.of(20, 40));
    }
}
