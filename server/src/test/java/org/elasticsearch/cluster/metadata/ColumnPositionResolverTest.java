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

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;

public class ColumnPositionResolverTest {

    // IndexTemplateUpgrader#populateColumnPositions is one of the main user of ColumnPositionResolver,
    // where populateColumnPositions method is a simple wrapper over it.
    // Therefore, see IndexTemplateUpgraderTest for more variations of ColumnPositionResolver tests.

    @Test
    public void test_once_updatePositions_called_the_resolver_is_cleared() {
        var t1 = new TestColumn(new String[]{"a"}, -2, 0);
        ColumnPositionResolver<TestColumn> resolver = new ColumnPositionResolver<>();
        addToResolver(resolver, t1);
        assertThat(resolver.numberOfColumnsToReposition()).isEqualTo(1);
        resolver.updatePositions(0);
        assertThat(resolver.columnsToReposition).isEqualTo(new TreeMap<>(Comparator.naturalOrder()));
        assertThat(resolver.takenPositions).isEqualTo(new HashMap<>());
        assertThat(resolver.numberOfColumnsToReposition()).isEqualTo(0);
        assertThat(t1.position).isEqualTo(1);
    }

    private static class TestColumn {
        String[] name;
        Integer columnOrder;
        int position;
        int depth;

        public TestColumn(String[] name, Integer columnOrder, int depth) {
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
