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

import static io.crate.testing.Asserts.assertThat;

import org.junit.Before;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;

public class WhereClauseTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions sqlExpressions;

    @Before
    public void prepare() {
        sqlExpressions = new SqlExpressions(T3.sources(clusterService));
    }

    @Test
    public void testAddWithNoMatch() {
        WhereClause where1 = WhereClause.NO_MATCH;
        WhereClause where2 = new WhereClause(sqlExpressions.asSymbol("x = 10"));
        WhereClause where1_where2 = where1.add(where2.query());

        assertThat(where1_where2.queryOrFallback()).isLiteral(false);
    }

    @Test
    public void testAddWithMatchAll() {
        WhereClause where1 = WhereClause.MATCH_ALL;
        WhereClause where2 = new WhereClause(sqlExpressions.asSymbol("x = 10"));
        WhereClause where1_where2 = where1.add(where2.query());

        assertThat(where1_where2.hasQuery()).isTrue();
        assertThat(where1_where2.query()).isEqualTo(where2.query());
    }

    @Test
    public void testAddWithNullQuery() {
        WhereClause where1 = new WhereClause(null);
        WhereClause where2 = new WhereClause(sqlExpressions.asSymbol("x = 10"));
        WhereClause where1_where2 = where1.add(where2.query());

        assertThat(where1_where2.hasQuery()).isTrue();
        assertThat(where1_where2.query()).isEqualTo(where2.query());
    }
}
