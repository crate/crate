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

package io.crate.analyze.where;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

import io.crate.metadata.ColumnIdent;

public class ColumnsUnderOrOperatorFinderTest extends EqualityExtractorBaseTest {

    @Test
    public void test_find_nonPK_column_under_or_first_level() {
        var query = query("x = 1 or i = 1");
        var columns = List.of(new ColumnIdent("x"));
        assertThat(new EqualityExtractor.ColumnsUnderOrOperatorFinder().find(query, columns)).isTrue();
    }

    @Test
    public void test_find_nonPK_column_under_or_and_neg() {
        var query = query("x = 1 or not i = 1");
        var columns = List.of(new ColumnIdent("x"));
        assertThat(new EqualityExtractor.ColumnsUnderOrOperatorFinder().find(query, columns)).isTrue();
    }

    @Test
    public void test_find_nonPK_column_under_or_complex_query() {
        var query = query("x = 1 and (x = 2 and (not(x = 3) or i = 1) and x = 4)");
        var columns = List.of(new ColumnIdent("x"));
        assertThat(new EqualityExtractor.ColumnsUnderOrOperatorFinder().find(query, columns)).isTrue();
    }

    @Test
    public void test_cannot_find_nonPK_column_under_and_complex_query() {
        var query = query("x = 1 and (x = 2 and not((x = 3 and i = 1) or x = 4))");
        var columns = List.of(new ColumnIdent("x"));
        assertThat(new EqualityExtractor.ColumnsUnderOrOperatorFinder().find(query, columns)).isFalse();
    }
}
