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

package io.crate.expression.reference.doc;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.QueryTester;
import org.elasticsearch.Version;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;
import java.util.stream.IntStream;

public class IntegerColumnReferenceTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testIntegerExpression() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            createTempDir(),
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table t (x int)"
        );
        builder.indexValues("x", IntStream.range(-2, 3).boxed().toArray());
        try (QueryTester tester = builder.build()) {
            List<Object> objects = tester.runQuery("x", "true");
            assertThat(objects, Matchers.contains(-2, -1, 0, 1, 2));
        }
    }
}
