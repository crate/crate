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

package io.crate.execution.engine.aggregation.impl;

import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.test.IntegTestCase;

public class TopKIntegrationTest extends IntegTestCase {

    public void test_default_case() {
        execute("create table doc.t1(x long);");
        execute("insert into doc.t1 values (1), (2), (2), (3), (3), (3);");
        execute("refresh table doc.t1;");
        execute("select top_k(x) from doc.t1;");
        assertThat(response).hasRows("[{item=3, frequency=3}, {item=2, frequency=2}, {item=1, frequency=1}]");
    }

    public void test_with_limit() {
        execute("create table doc.t1(x long);");
        execute("insert into doc.t1 values (1), (2), (2), (3), (3), (3);");
        execute("refresh table doc.t1;");
        execute("select top_k(x, 2) from doc.t1;");
        assertThat(response).hasRows("[{item=3, frequency=3}, {item=2, frequency=2}]");
    }
}
