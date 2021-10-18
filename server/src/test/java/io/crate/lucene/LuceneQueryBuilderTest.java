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

package io.crate.lucene;

import io.crate.expression.symbol.Symbol;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.IndexVersionCreated;
import io.crate.testing.QueryTester;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.lang.reflect.Method;

public abstract class LuceneQueryBuilderTest extends CrateDummyClusterServiceUnitTest {

    @Rule
    public TestName testName = new TestName();

    private QueryTester queryTester;

    @Before
    public void prepare() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            createTempDir(),
            THREAD_POOL,
            clusterService,
            indexVersion(),
            "create table users (" +
            " name string," +
            " tags string index using fulltext not null," +
            " content string index using fulltext," +
            " x integer not null," +
            " f float," +
            " d double," +
            " obj object as (" +
            "     x integer," +
            "     y integer" +
            " )," +
            " obj_ignored object (ignored), " +
            " d_array array(double)," +
            " y_array array(long)," +
            " o_array array(object as (xs array(integer)))," +
            " ts_array array(timestamp with time zone)," +
            " shape geo_shape," +
            " point geo_point," +
            " ts timestamp with time zone," +
            " addr ip," +
            " vchar_name varchar(40)," +
            " byte_col byte, " +
            " bool_col boolean " +
            ")"
        );
        queryTester = builder.build();
    }

    @After
    public void tearDownQueryTester() throws Exception {
        queryTester.close();
    }

    protected Query convert(String expression) {
        return queryTester.toQuery(expression);
    }

    protected Query convert(String expression, Object ... params) {
        return queryTester.toQuery(expression, params);
    }

    protected Query convert(Symbol expression) {
        return queryTester.toQuery(expression);
    }

    private Version indexVersion() {
        try {
            Class<?> clazz = this.getClass();
            Method method = clazz.getMethod(testName.getMethodName());
            IndexVersionCreated annotation = method.getAnnotation(IndexVersionCreated.class);
            if (annotation == null) {
                annotation = clazz.getAnnotation(IndexVersionCreated.class);
                if (annotation == null) {
                    return Version.CURRENT;
                }
            }
            int value = annotation.value();
            if (value == -1) {
                return Version.CURRENT;
            }
            return Version.fromId(value);
        } catch (NoSuchMethodException ignored) {
            return Version.CURRENT;
        }
    }
}
