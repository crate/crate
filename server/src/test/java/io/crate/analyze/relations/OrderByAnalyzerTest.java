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

package io.crate.analyze.relations;

import static io.crate.testing.Asserts.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.SortItem;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class OrderByAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void analyzeEmptySortItemsReturnsNull() {
        assertThat(OrderyByAnalyzer.analyzeSortItems(Collections.emptyList(), null)).isNull();
    }

    @Test
    public void analyzeSortItems() {
        List<SortItem> sortItems = new ArrayList<>(2);
        QualifiedName tx = QualifiedName.of("t", "x");
        SortItem firstSort =
            new SortItem(new QualifiedNameReference(tx), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.FIRST);
        sortItems.add(firstSort);
        QualifiedName ty = QualifiedName.of("t", "y");
        SortItem second =
            new SortItem(new QualifiedNameReference(ty), SortItem.Ordering.DESCENDING, SortItem.NullOrdering.LAST);
        sortItems.add(second);

        OrderBy orderBy = OrderyByAnalyzer.analyzeSortItems(sortItems,
            e -> Literal.of(((QualifiedNameReference) e).getName().toString()));

        assertThat(orderBy).isNotNull();
        List<Symbol> orderBySymbols = orderBy.orderBySymbols();
        assertThat(orderBySymbols).satisfiesExactly(
            s -> assertThat(s).isLiteral("t.x"),
            s -> assertThat(s).isLiteral("t.y"));

        boolean[] reverseFlags = orderBy.reverseFlags();
        assertThat(reverseFlags).containsExactly(false, true);

        boolean[] nullsFirst = orderBy.nullsFirst();
        assertThat(nullsFirst).containsExactly(true, false);
    }

    @Test
    public void test_order_by_null_ordinal_with_explicit_cast() {
        var e = SQLExecutor.builder(clusterService)
            .build();
        QueriedSelectRelation relation = e.analyze("select * from unnest([1, 2]) order by null::integer");
        assertThat(relation.orderBy()).isSQL("NULL");
    }
}
