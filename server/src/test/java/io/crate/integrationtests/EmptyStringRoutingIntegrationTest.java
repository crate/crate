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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.nio.file.Paths;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.testing.SQLResponse;
import io.crate.testing.UseRandomizedOptimizerRules;

/**
 * In ElasticSearch a routing value of '' is treated as if there is no routing
 * (see https://github.com/elasticsearch/elasticsearch/issues/1450). This behaviour leads to
 * inconsistent routings, since in the case of a missing routing the _id is used as fallback. This would break
 * the contract of having the same value of the routing field on only one shard. So in Crate the behaviour is
 * different and is asserted via tests in this class.
 */
public class EmptyStringRoutingIntegrationTest extends IntegTestCase {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testInsertEmtpyStringRoutingByValue() throws Exception {
        execute("create table t (i int primary key, c string primary key) clustered by (c)");
        ensureYellow();
        execute("insert into t (i, c) values (1, '')");
        execute("insert into t (i, c) values (2, '')");
        refresh();
        execute("select c, count(*) from t group by c");
        assertThat(response.rowCount(), is(1L));
        assertThat((long) response.rows()[0][1], is(2L));
    }

    @Test
    public void testInsertEmtpyStringRoutingByMultiValue() throws Exception {
        execute("create table t (i int primary key, c string primary key) clustered by (c)");
        ensureYellow();
        execute("insert into t (i, c) values (1, ''), (2, '')");
        refresh();
        execute("select c, count(*) from t group by c");
        assertThat(response.rowCount(), is(1L));
        assertThat((long) response.rows()[0][1], is(2L));
    }

    @Test
    public void testInsertEmtpyStringRoutingByArgs() throws Exception {
        execute("create table t (i int primary key, c string primary key) clustered by (c)");
        ensureYellow();
        execute("insert into t (i, c) values (?, ?)", new Object[]{1, ""});
        execute("insert into t (i, c) values (?, ?)", new Object[]{2, ""});
        ;
        refresh();
        execute("select c, count(*) from t group by c");
        assertThat(response.rowCount(), is(1L));
        assertThat((long) response.rows()[0][1], is(2L));
    }

    @Test
    public void testInsertEmtpyStringRoutingByBulkArgs() throws Exception {
        execute("create table t (i int primary key, c string primary key) clustered by (c)");
        ensureYellow();
        execute("insert into t (i, c) values (?, ?)", new Object[][]{{1, ""}, {2, ""}});
        refresh();
        execute("select c, count(*) from t group by c");
        assertThat(response.rowCount(), is(1L));
        assertThat((long) response.rows()[0][1], is(2L));
    }

    @Test
    @UseRandomizedOptimizerRules(0) // depends on primary key lookup
    public void testInsertEmtpyStringRoutingIsRealtime() throws Exception {
        execute("create table t (i int primary key, c string primary key, a int)" +
                " clustered by (c) with (refresh_interval=-1)");
        ensureYellow();

        execute("insert into t (i, c) values (1, '')");
        execute("select i from t where i=1 and c=''");
        assertThat(response.rowCount(), is(1L));

        execute("update t set a=5 where i=1 and c=''");
        execute("select a from t where i=1 and c=''");
        assertThat(response.rowCount(), is(1L));
        assertThat((int) response.rows()[0][0], is(5));
    }

    @Test
    public void testCopyFromEmptyStringRouting() throws Exception {
        execute("create table t (i int primary key, c string primary key, a int) clustered by (c)");
        ensureYellow();
        execute("insert into t (i, c) values (1, ''), (2, '')");
        refresh();

        String uri = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy t to directory ?", new Object[]{uri});
        assertThat(response.rowCount(), is(2L));

        execute("delete from t");
        refresh();

        execute("copy t from ? with (shared=true)", new Object[]{uri + "t_*"});
        refresh();
        response = execute("select c, count(*) from t group by c");
        assertThat(response.rowCount(), is(1L));
        assertThat((long) response.rows()[0][1], is(2L));

    }
}
