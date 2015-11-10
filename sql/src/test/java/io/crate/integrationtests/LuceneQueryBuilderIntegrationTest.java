/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.integrationtests;

import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

@ElasticsearchIntegrationTest.ClusterScope (randomDynamicTemplates = false)
public class LuceneQueryBuilderIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testWhereFunctionWithAnalyzedColumnArgument() throws Exception {
        execute("create table t (text string index using fulltext) " +
                "clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (text) values ('hello world')");
        refresh();

        execute("select text from t where substr(text, 1, 1) = 'h'");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testEqualsQueryOnArrayType() throws Exception {
        execute("create table t (a array(integer)) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (a) values (?)", new Object[][]{
                new Object[]{new Object[] {10, 10, 20}},
                new Object[]{new Object[] {40, 50, 60}},
                new Object[]{new Object[] {null, null}}
        });
        execute("refresh table t");

        execute("select * from t where a = [10, 10, 20]");
        assertThat(response.rowCount(), is(1L));

        execute("select * from t where a = [10, 20]");
        assertThat(response.rowCount(), is(0L));

        execute("select * from t where a = [null, null]");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testWhereFunctionWithIndexOffColumn() throws Exception {
        execute("create table t (text string index off) " +
                "clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (text) values ('hello world')");
        refresh();

        execute("select text from t where substr(text, 1, 1) = 'h'");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testWhereFunctionWithIndexReference() throws Exception {
        execute("create table t (text string, index text_ft using fulltext (text)) " +
                "clustered into 2 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (text) values ('hello world')");
        execute("insert into t (text) values ('harr')");
        execute("insert into t (text) values ('hh')");
        refresh();

        execute("select text from t where substr(text_ft, 1, 1) = 'h'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testWhereSubstringWithSysColumn() throws Exception {
        execute("create table t (dummy string) clustered into 2 shards with (number_of_replicas = 1)");
        ensureYellow();
        execute("insert into t (dummy) values ('yalla')");
        refresh();

        execute("select dummy from t where substr(_raw, 1, 1) = '{'");
        assertThat(response.rowCount(), is(1L));
        assertThat(((String) response.rows()[0][0]), is("yalla"));
    }

    @Test
    public void testInWithArgs() throws Exception {
        execute("create table t (i int) clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t values (1), (2)");
        execute("refresh table t");

        StringBuilder sb = new StringBuilder("select i from t where i in (");

        int i=0;
        for (; i < 1500; i++) {
            sb.append(i);
            sb.append(',');
        }
        sb.append(i);
        sb.append(')');

        execute(sb.toString());
        assertThat(response.rowCount(), is(2L));
    }

    @Test
    public void testWithinGenericFunction() throws Exception {
        execute("create table shaped (id int, point geo_point, shape geo_shape) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into shaped (id, point, shape) VALUES (?, ?, ?)", $$(
                $(1, "POINT (15 15)", "polygon (( 10 10, 10 20, 20 20, 20 15, 10 10))"),
                $(1, "POINT (-10 -10)", "polygon (( 10 10, 10 20, 20 20, 20 15, 10 10))")
        ));
        execute("refresh table shaped");

        execute("select * from shaped where within(point, shape) order by id");
        assertThat(response.rowCount(), is(1L));

    }
}
