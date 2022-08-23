/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package org.elasticsearch.cluster.metadata;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ColumnPositionResolverTest {

    @Test
    public void test_column_positions_resolved_by_depths_first() {
        var t1 = new TestColumn("a", -1, 1);
        var t2 = new TestColumn("a", -1, 0);
        ColumnPositionResolver<TestColumn> resolver = new ColumnPositionResolver<>();
        addToResolver(resolver, t1);
        addToResolver(resolver, t2);
        resolver.updatePositions(0);
        assertThat(t2.position).isEqualTo(1);
        assertThat(t1.position).isEqualTo(2);
    }

    @Test
    public void test_column_positions_resolved_by_column_order_if_depths_are_the_same() {
        var t1 = new TestColumn("a", -2, 0);
        var t2 = new TestColumn("a", -1, 0);
        ColumnPositionResolver<TestColumn> resolver = new ColumnPositionResolver<>();
        addToResolver(resolver, t1);
        addToResolver(resolver, t2);
        resolver.updatePositions(0);
        assertThat(t2.position).isEqualTo(1);
        assertThat(t1.position).isEqualTo(2);
    }

    @Test
    public void test_column_positions_resolved_by_names_if_depths_and_column_orders_are_the_same() {
        var t1 = new TestColumn("b", -2, 0);
        var t2 = new TestColumn("a", -2, 0);
        ColumnPositionResolver<TestColumn> resolver = new ColumnPositionResolver<>();
        addToResolver(resolver, t1);
        addToResolver(resolver, t2);
        resolver.updatePositions(0);
        assertThat(t2.position).isEqualTo(1);
        assertThat(t1.position).isEqualTo(2);
    }

    private static class TestColumn {
        String name;
        Integer columnOrder;
        int position;
        int depth;

        public TestColumn(String name, Integer columnOrder, int depth) {
            this.name = name;
            this.columnOrder = columnOrder;
            this.depth = depth;
        }
    }

    private static void addToResolver(ColumnPositionResolver<TestColumn> resolver, TestColumn col) {
        resolver.addColumnToReposition(
            col.name,
            col.columnOrder,
            col,
            (c, i) -> c.position = i,
            col.depth
        );
    }
}
