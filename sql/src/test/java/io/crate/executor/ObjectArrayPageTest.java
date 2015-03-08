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

package io.crate.executor;

import com.google.common.collect.Iterators;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.is;

public class ObjectArrayPageTest extends CrateUnitTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testEmptyObjectArray() throws Exception {
        ObjectArrayPage page = new ObjectArrayPage(new Object[0][], 0, 0);
        assertThat(page.size(), is(0L));
        assertThat(page.iterator().hasNext(), is(false));

        page = new ObjectArrayPage(new Object[0][], 0, 10);
        assertThat(page.size(), is(0L));
        assertThat(page.iterator().hasNext(), is(false));
    }

    @Test
    public void testBigObjectArray() throws Exception {
        Object[][] pageSource = new Object[100][];
        for (int i = 0; i < pageSource.length; i++) {
            pageSource[i] = new Object[] { i };
        }
        ObjectArrayPage page = new ObjectArrayPage(pageSource, 0, 10);
        assertThat(page.size(), is(10L));
        assertThat(Iterators.size(page.iterator()), is(10));

        page = new ObjectArrayPage(pageSource, 10, 10);
        assertThat(page.size(), is(10L));
        assertThat(Iterators.size(page.iterator()), is(10));

        page = new ObjectArrayPage(pageSource, 0, 100);
        assertThat(page.size(), is(100L));
        assertThat(Iterators.size(page.iterator()), is(100));

        page = new ObjectArrayPage(pageSource, 10, 100);
        assertThat(page.size(), is(90L));
        assertThat(Iterators.size(page.iterator()), is(90));

        page = new ObjectArrayPage(pageSource);
        assertThat(page.size(), is(100L));
        assertThat(Iterators.size(page.iterator()), is(100));
    }

    @Test
    public void testExceed() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("start exceeds page");
        new ObjectArrayPage(new Object[2][], 3, 1);
    }


}
