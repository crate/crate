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

package io.crate.integrationtests;

import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsArrayContainingInOrder.arrayContaining;


public class CastIntegrationTest extends SQLTransportIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testTryCastValidLiteralCasting() {
        execute("select try_cast('2' as integer), try_cast(['1', '2'] as array(integer)) from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        assertThat((int) response.rows()[0][0], is(2));
        assertThat(response.rows()[0][1], (Matcher) arrayContaining(1, 2));
    }

    @Test
    public void testTryCastNotValidLiteralCasting() {
        execute("select try_cast('2e' as integer), try_cast('1' as boolean), try_cast(128 as byte) from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(nullValue()));
        assertThat(response.rows()[0][1], is(nullValue()));
        assertThat(response.rows()[0][2], is(nullValue()));
    }

    @Test
    public void testTryCastReturnNullWhenCastingFailsOnRows() {
        execute("create table types (i integer, str string)");
        execute("insert into types (i, str) values (?, ?)", new Object[]{1, "33"});
        execute("insert into types (i, str) values (?, ?)", new Object[]{2, "3d"});
        refresh();
        execute("select try_cast(i as integer), try_cast(str as integer) from types order by i asc");
        assertThat(response.rowCount(), is(2L));
        assertThat(((int) response.rows()[0][0]), is(1));
        assertThat(((int) response.rows()[0][1]), is(33));
        assertThat(((int) response.rows()[1][0]), is(2));
        assertThat(response.rows()[1][1], is(nullValue()));
    }

    @Test
    public void testInvalidCastExpression() throws Exception {
        execute("create table types (str string)");
        execute("insert into types (str) values (?)", new Object[]{"string"});
        refresh();
        expectedException.expect(Exception.class);
        expectedException.expectMessage("No cast function found for return type object");
        execute("select try_cast(str as object) from types");
    }
}
