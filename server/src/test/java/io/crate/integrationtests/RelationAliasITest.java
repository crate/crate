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

package io.crate.integrationtests;

import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

public class RelationAliasITest extends IntegTestCase {

    @Test
    public void testRelationAliasWithColumnAliases() {
        execute("""
            SELECT
                y,
                avg(x) OVER (ORDER BY x),
                avg(y) OVER ()
            FROM
                unnest(array[1, 2, 3, 4, 5, 6, 7], array[10, 20, 30, 40, 50, 60, 70]) as t (x, y)
            ORDER BY
                y
            LIMIT 3 offset 2""");
        assertThat(response).hasRows(
            "30| 2.0| 40.0",
            "40| 2.5| 40.0",
            "50| 3.0| 40.0");
    }

    @Test
    public void testRelationAliasWithSomeColumnsAliases() {
        execute("""
            SELECT
                col2,
                avg(x) OVER (ORDER BY x),
                avg(col2) OVER ()
            FROM
                unnest(array[1, 2, 3, 4, 5, 6, 7], array[10, 20, 30, 40, 50, 60, 70]) as t (x)
            ORDER BY
                col2
            LIMIT 3 offset 2""");
        assertThat(response).hasRows(
            "30| 2.0| 40.0",
            "40| 2.5| 40.0",
            "50| 3.0| 40.0");
    }

    @Test
    public void testSameColumnWithDifferentOutputNamesAndARelationBoundary() {
        execute("create table t1 (id int, a text)");
        execute("insert into t1 (id, a) values (1, 'foo'), (2, 'foo'), (3, 'bar')");
        execute("refresh table t1");
        execute("select count(*) as cnt, t.a as a_alias, t.a" +
                " from t1 t" +
                " group by 2" +
                " order by 1");
        assertThat(response).hasRows(
            "1| bar| bar",
            "2| foo| foo");
    }
}
