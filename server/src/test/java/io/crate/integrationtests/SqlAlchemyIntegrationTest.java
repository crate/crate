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

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

public class SqlAlchemyIntegrationTest extends IntegTestCase {

    @Test
    public void testSqlAlchemyGeneratedCountWithStar() throws Exception {
        // generated using sqlalchemy
        // session.query(func.count('*')).filter(Test.name == 'foo').scalar()

        execute("create table test (col1 integer primary key, col2 string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test values (?, ?)", new Object[]{1, "foo"});
        execute("insert into test values (?, ?)", new Object[]{2, "bar"});
        execute("refresh table test");

        execute(
            "SELECT count(?) AS count_1 FROM test WHERE test.col2 = ?",
            new Object[]{"*", "foo"}
        );
        assertThat(response.rows()[0][0]).isEqualTo(1L);
    }

    @Test
    public void testSqlAlchemyGeneratedCountWithPrimaryKeyCol() throws Exception {
        // generated using sqlalchemy
        // session.query(Test.col1).filter(Test.col2 == 'foo').scalar()

        execute("create table test (col1 integer primary key, col2 string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test values (?, ?)", new Object[]{1, "foo"});
        execute("insert into test values (?, ?)", new Object[]{2, "bar"});
        execute("refresh table test");

        execute(
            "SELECT count(test.col1) AS count_1 FROM test WHERE test.col2 = ?",
            new Object[]{"foo"}
        );
        assertThat(response.rows()[0][0]).isEqualTo(1L);
    }

    @Test
    public void testSqlAlchemyGroupByWithCountStar() throws Exception {
        // generated using sqlalchemy
        // session.query(func.count('*'), Test.col2).group_by(Test.col2).order_by(desc(func.count('*'))).all()

        execute("create table test (col1 integer primary key, col2 string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test values (?, ?)", new Object[]{1, "foo"});
        execute("insert into test values (?, ?)", new Object[]{2, "bar"});
        execute("insert into test values (?, ?)", new Object[]{3, "foo"});
        execute("refresh table test");

        execute(
            "SELECT count(?) AS count_1, test.col2 AS test_col2 FROM test " +
            "GROUP BY test.col2 order by count_1 desc",
            new Object[]{"*"}
        );

        assertThat(response.rows()[0][0]).isEqualTo(2L);
    }

    @Test
    public void testSqlAlchemyGroupByWithPrimaryKeyCol() throws Exception {
        // generated using sqlalchemy
        // session.query(func.count(Test.col1), Test.col2).group_by(Test.col2).order_by(desc(func.count(Test.col1))).all()


        execute("create table test (col1 integer primary key, col2 string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into test values (?, ?)", new Object[]{1, "foo"});
        execute("insert into test values (?, ?)", new Object[]{2, "bar"});
        execute("insert into test values (?, ?)", new Object[]{3, "foo"});
        execute("refresh table test");

        execute(
            "SELECT count(test.col1) AS count_1, test.col2 AS test_col2 FROM test " +
            "GROUP BY test.col2 order by count_1 desc"
        );

        assertThat(response.rows()[0][0]).isEqualTo(2L);
    }
}
