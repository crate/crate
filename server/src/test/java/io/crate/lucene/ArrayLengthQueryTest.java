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

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.DataTypeTesting;
import io.crate.testing.QueryTester;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.elasticsearch.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;

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
    public void testArrayLengthGt0FiltersEmptyAndNullRecords() throws Exception {
        List<Object> rows = tester.runQuery("xs", "array_length(xs, 1) > 0");
        assertThat(
            rows,
            containsInAnyOrder(
                List.of(10),
                List.of(20),
                List.of(10, 10),
                List.of(10, 20),
                List.of(10, 10, 20),
                List.of(10, 20, 30)
            )
        );
    }

    @Test
    public void testArrayLengthGte0FiltersZeroLengthAndNullRecords() throws Exception {
        // array_upper([], 1) evaluates to NULL so NULL >= 0 -> no match
        List<Object> rows = tester.runQuery("xs", "array_length(xs, 1) >= 0");
        assertThat(
            rows,
            containsInAnyOrder(
                List.of(10),
                List.of(20),
                List.of(10, 10),
                List.of(10, 20),
                List.of(10, 10, 20),
                List.of(10, 20, 30)
            )
        );
    }

    @Test
    public void testArrayLengthGt1FiltersNullEmptyAndLength1Records() throws Exception {
        List<Object> rows = tester.runQuery("xs", "array_length(xs, 1) > 1");
        assertThat(
            rows,
            containsInAnyOrder(
                List.of(10, 10),
                List.of(10, 20),
                List.of(10, 10, 20),
                List.of(10, 20, 30)
            )
        );
    }

    @Test
    public void testArrayLengthGte1ReturnsAllButEmptyOrNullValues() throws Exception {
        List<Object> rows = tester.runQuery("xs", "array_length(xs, 1) >= 1");
        assertThat(
            rows,
            containsInAnyOrder(
                List.of(10),
                List.of(20),
                List.of(10, 10),
                List.of(10, 20),
                List.of(10, 10, 20),
                List.of(10, 20, 30)
            )
        );
    }

    @Test
    public void testArrayLengthGt2ReturnsAllGreater2() throws Exception {
        List<Object> rows = tester.runQuery("xs", "array_length(xs, 1) > 2");
        assertThat(
            rows,
            containsInAnyOrder(
                List.of(10, 10, 20),
                List.of(10, 20, 30)
            )
        );
    }

    @Test
    public void testArrayLengthGte2ReturnsAllGreaterOrEq2() throws Exception {
        List<Object> rows = tester.runQuery("xs", "array_length(xs, 1) >= 2");
        assertThat(
            rows,
            containsInAnyOrder(
                List.of(10, 10),
                List.of(10, 20),
                List.of(10, 10, 20),
                List.of(10, 20, 30)
            )
        );
    }

    @Test
    public void testArrayLengthLt0ReturnsNothing() throws Exception {
        List<Object> rows = tester.runQuery("xs", "array_length(xs, 1) < 0");
        assertThat(
            rows,
            empty()
        );
    }

    @Test
    public void testArrayLengthLte0ReturnsNothing() throws Exception {
        // `array_length([], 1)` <= 0 --> `NULL <= 0` --> NO MATCH
        List<Object> rows = tester.runQuery("xs", "array_length(xs, 1) <= 0");
        assertThat(
            rows,
            empty()
        );
    }

    @Test
    public void testArrayLengthLt1ReturnsNothing() throws Exception {
        // Since `array_length([], 1)` returns NULL, there can't be a match for < 1
        List<Object> rows = tester.runQuery("xs", "array_length(xs, 1) < 1");
        assertThat(
            rows,
            empty()
        );
    }

    @Test
    public void testArrayLengthLte1ReturnsArraysWith1Element() throws Exception {
        List<Object> rows = tester.runQuery("xs", "array_length(xs, 1) <= 1");
        assertThat(
            rows,
            containsInAnyOrder(
                List.of(10),
                List.of(20)
            )
        );
    }

    @Test
    public void testArrayLengthLte3ReturnsArraysWithUpToIncl3Element() throws Exception {
        List<Object> rows = tester.runQuery("xs", "array_length(xs, 1) <= 3");
        assertThat(
            rows,
            containsInAnyOrder(
                List.of(10),
                List.of(20),
                List.of(10, 10),
                List.of(10, 20),
                List.of(10, 10, 20),
                List.of(10, 20, 30)
            )
        );
    }

    @Test
    public void testArrayLengthLt3ReturnsArraysWithUpToExcl3Element() throws Exception {
        List<Object> rows = tester.runQuery("xs", "array_length(xs, 1) < 3");
        assertThat(
            rows,
            containsInAnyOrder(
                List.of(10),
                List.of(20),
                List.of(10, 10),
                List.of(10, 20)
            )
        );
    }

    @Test
    public void testArrayLengthEq1ReturnsArraysWith1Element() throws Exception {
        List<Object> rows = tester.runQuery("xs", "array_length(xs, 1) = 1");
        assertThat(
            rows,
            containsInAnyOrder(
                List.of(10),
                List.of(20)
            )
        );
    }

    @Test
    public void testArrayLengthEq1ReturnsArraysWith2Elements() throws Exception {
        List<Object> rows = tester.runQuery("xs", "array_length(xs, 1) = 2");
        assertThat(
            rows,
            containsInAnyOrder(
                List.of(10, 10),
                List.of(10, 20)
            )
        );
    }

    @Test
    public void testArrayLengthEq0ReturnsNoElements() throws Exception {
        // Since `array_length([], 1)` returns NULL, there can't be a match for = 0
        List<Object> rows = tester.runQuery("xs", "array_length(xs, 1) = 0");
        assertThat(
            rows,
            empty()
        );
    }


    @Test
    public void testArrayLengthWithAllSupportedTypes() throws Exception {
        for (DataType<?> type : DataTypeTesting.ALL_TYPES_EXCEPT_ARRAYS) {
            // This is temporary as long as interval is not fully implemented
            if (type.storageSupport() == null) {
                continue;
            }
            Supplier dataGenerator = DataTypeTesting.getDataGenerator(type);
            Object val1 = dataGenerator.get();
            Object val2 = dataGenerator.get();
            Object[] arr = {val1, val2};
            Object[] values = new Object[] {
                arr
            };

            // ensure the test is operating on a fresh, empty cluster state (no tables)
            resetClusterService();

            try (QueryTester tester = new QueryTester.Builder(
                createTempDir(),
                THREAD_POOL,
                clusterService,
                Version.CURRENT,
                "create table \"t_"+ type.getName() + "\" (xs array(\"" + type.getName() + "\"))"
            ).indexValues("xs", values).build()) {
                System.out.println(type);
                List<Object> result = tester.runQuery("xs", "array_length(xs, 1) >= 2");
                assertThat(result.size(), is(1));
                ArrayType arrayType = new ArrayType<>(type);
                // Object compareValueTo does type-guessing which might result in
                // double/float conversions which are not fully accurate, so we skip that here
                // having the result size check should be sufficient anyway
                if (type.id() != ObjectType.ID) {
                    assertThat(arrayType.compare((List) result.get(0), Arrays.asList(arr)), is(0));
                }
            }
        }
    }
}
