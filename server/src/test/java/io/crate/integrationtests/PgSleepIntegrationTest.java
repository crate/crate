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

public class PgSleepIntegrationTest extends IntegTestCase {

    @Test
    public void test_pg_sleep() {
        long before = System.currentTimeMillis();
        execute("select pg_sleep(1.0);");
        long after = System.currentTimeMillis();
        assertThat(after - before >= 1000).isTrue();
    }

    @Test
    public void test_pg_sleep_wrong_arguments() {
        try {
            execute("select pg_sleep(1, 1);");
            assertThat(true).isEqualTo(false); // shouldn't reach this line
        } catch (Exception e) {
            assertThat(e.getMessage()).contains("Invalid arguments in: pg_sleep(1, 1) with (integer, integer). Valid types: (double precision)");
        }
    }
}
