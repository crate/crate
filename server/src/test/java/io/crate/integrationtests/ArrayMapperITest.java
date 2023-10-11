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

import java.util.List;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

public class ArrayMapperITest extends IntegTestCase {

    @Test
    public void testInsertGetArray() throws Exception {
        execute("create table t (id int primary key, xs array(double))");
        execute("insert into t (id, xs) values (1, [0.0, 99.9, -100.5678])");
        execute("refresh table t");

        execute("select xs from t where id = 1"); // pk lookup
        assertThat(response).hasRows(new Object[]{List.of(0.0d, 99.9d, -100.5678d)});

        execute("select xs from t");
        assertThat(response).hasRows(new Object[]{List.of(0.0d, 99.9d, -100.5678d)});
    }

    @Test
    public void testInsertSearchArray() throws Exception {
        execute("create table t (xs array(double))");
        execute("insert into t (xs) values ([0.0, 99.9, -100.5678])");
        execute("refresh table t");

        execute("select xs from t where 0.0 = any (xs)");
        assertThat(response).hasRows(new Object[]{List.of(0.0d, 99.9d, -100.5678d)});
    }

    @Test
    public void testEmptyArray() throws Exception {
        execute("create table t (xs array(double))");
        execute("insert into t (xs) values ([])");
        execute("refresh table t");

        execute("select xs from t");
        assertThat(response).hasRows(new Object[]{List.of()});
    }
}
