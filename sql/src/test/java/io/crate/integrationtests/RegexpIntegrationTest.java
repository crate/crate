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

import io.crate.test.integration.CrateIntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class RegexpIntegrationTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testRegexpMatchesIsNull() throws Exception {
        execute("create table regex_test (i integer, s string) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into regex_test(i, s) values (?, ?)", new Object[][]{
                new Object[]{1, "foo is first"},
                new Object[]{2, "bar is second"},
                new Object[]{3, "foobar is great"},
                new Object[]{4, "crate is greater"},
                new Object[]{5, "foo"},
                new Object[]{6, null}
        });
        refresh();
        execute("select i from regex_test where regexp_matches(s, 'is') is not null");
        assertThat(response.rowCount(), is(4L));
        //execute("select i from regex_test where regexp_matches(s, 'is') is null");
        //assertThat(response.rowCount(), is(2L));
    }

    @Test
    public void testRegexpReplaceIsNull() throws Exception {
        execute("create table regex_test (i integer, s string) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into regex_test(i, s) values (?, ?)", new Object[][]{
                new Object[]{1, "foo is first"},
                new Object[]{2, "bar is second"},
                new Object[]{3, "foobar is great"},
                new Object[]{4, "crate is greater"},
                new Object[]{5, "foo"},
                new Object[]{6, null}
        });
        refresh();
        execute("select i from regex_test where regexp_replace(s, 'is', 'was') is not null");
        assertThat(response.rowCount(), is(5L));
        execute("select i from regex_test where regexp_replace(s, 'is', 'was') is null");
        assertThat(response.rowCount(), is(1L));
    }

}


