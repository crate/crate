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

package io.crate.sql.tree;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

public class JoinUsingTests {

    @Test
    public void testToExpression() {
        for (int n :List.of(1, 2, 3, 4, 5, 6, 7)) {

            List<String> cols = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                cols.add("col_" + i);
            }
            QualifiedName left = QualifiedName.of("doc", "t1");
            QualifiedName right = QualifiedName.of("doc", "t2");
            Expression e = JoinUsing.toExpression(left, right, cols);
            for (int i = 0; i < n - 2; i++) {
                assertThat(e).isExactlyInstanceOf(LogicalBinaryExpression.class);
                LogicalBinaryExpression and = (LogicalBinaryExpression) e;
                assertThat(and.getLeft()).isExactlyInstanceOf(ComparisonExpression.class);
                assertThat(and.getRight()).isExactlyInstanceOf(LogicalBinaryExpression.class);
                e = and.getRight();
            }
            if (1 == n) {
                assertThat(e).isExactlyInstanceOf(ComparisonExpression.class);
            } else {
                assertThat(e).isExactlyInstanceOf(LogicalBinaryExpression.class);
                LogicalBinaryExpression and = (LogicalBinaryExpression) e;
                assertThat(and.getLeft()).isExactlyInstanceOf(ComparisonExpression.class);
                assertThat(and.getRight()).isExactlyInstanceOf(ComparisonExpression.class);
            }
        }
    }
}
