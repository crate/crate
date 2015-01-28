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

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class RegexpIntegrationTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private Setup setup = new Setup(sqlExecutor);

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
        execute("select i from regex_test where regexp_matches(s, 'is') is null");
        assertThat(response.rowCount(), is(2L));
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

    @Test
    public void testRegexpMatchOperator() throws Exception {
        this.setup.setUpLocations();
        ensureGreen();
        refresh();
        execute("select distinct name from locations where name ~ '[A-Z][a-z0-9]+' order by name");
        assertThat(response.rowCount(), is(5L));
        assertThat((String) response.rows()[0][0], is("Aldebaran"));
        assertThat((String) response.rows()[1][0], is("Algol"));
        assertThat((String) response.rows()[2][0], is("Altair"));
        assertThat((String) response.rows()[3][0], is("Argabuthon"));
        assertThat((String) response.rows()[4][0], is("Bartledan"));

        execute("select name from locations where name !~ '[A-Z][a-z0-9]+' order by name");
        assertThat(response.rowCount(), is(8L));
        assertThat((String) response.rows()[0][0], is(""));
        assertThat((String) response.rows()[1][0], is("Allosimanius Syneca"));
        assertThat((String) response.rows()[2][0], is("Alpha Centauri"));
        assertThat((String) response.rows()[3][0], is("Arkintoofle Minor"));
        assertThat((String) response.rows()[4][0], is("Galactic Sector QQ7 Active J Gamma"));
        assertThat((String) response.rows()[5][0], is("North West Ripple"));
        assertThat((String) response.rows()[6][0], is("Outer Eastern Rim"));
        assertThat(response.rows()[7][0], is(nullValue()));
    }

}


