/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.lucene;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.QueryTester;
import org.elasticsearch.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class ArrayLengthQueryTest extends CrateDummyClusterServiceUnitTest {

    private QueryTester tester;

    @Before
    public void setUpTester() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            createTempDir(),
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table t (xs array(integer))"
        );
        tester = builder
            .indexValues(
                "xs",
                null,
                new Object[0],
                new Object[0],
                new Object[] { 10 },
                new Object[] { 20 },
                new Object[] { 10, 10 },
                new Object[] { 10, 20 },
                new Object[] { 10, 10, 20 },
                new Object[] { 10, 20, 30 }
            )
            .build();
    }

    @After
    public void tearDownTester() throws Exception {
        tester.close();
    }

    @Test
    public void testGt0FiltersEmptyAndNullRecords() throws Exception {
        List<Object> rows = tester.runQuery("xs", "array_upper(xs, 1) > 0");
        assertThat(
            rows,
            containsInAnyOrder(
                new Object[] { 10 },
                new Object[] { 20 },
                new Object[] { 10, 10 },
                new Object[] { 10, 20 },
                new Object[] { 10, 10, 20 },
                new Object[] { 10, 20, 30 }
            )
        );
    }

    @Test
    public void testGte0FiltersZeroLengthAndNullRecords() throws Exception {
        // array_upper([], 1) evaluates to NULL so NULL >= 0 -> no match
        List<Object> rows = tester.runQuery("xs", "array_upper(xs, 1) >= 0");
        assertThat(
            rows,
            containsInAnyOrder(
                new Object[] { 10 },
                new Object[] { 20 },
                new Object[] { 10, 10 },
                new Object[] { 10, 20 },
                new Object[] { 10, 10, 20 },
                new Object[] { 10, 20, 30 }
            )
        );
    }
}
