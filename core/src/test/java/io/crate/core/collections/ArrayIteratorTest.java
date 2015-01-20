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

package io.crate.core.collections;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;

public class ArrayIteratorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Object[][] createArray(int length) {
        Object[][] backingArray = new Object[length][];
        for (int i = 0; i< length; i++) {
            backingArray[i] = new Object[] { i };
        }
        return backingArray;
    }

    @Test
    public void testIterFull() throws Exception {
        Object[][] array = createArray(10);
        ArrayIterator<Object[]> iter = new ArrayIterator<>(array, 0, array.length);

        Object[][] itered = new Object[10][];
        int i = 0;
        while (iter.hasNext()) {
            itered[i] = iter.next();
            i++;
        }
        assertThat(itered, is(arrayContaining(array)));
    }

    @Test
    public void testIterLess() throws Exception {
        Object[][] array = createArray(10);
        ArrayIterator<Object[]> iter = new ArrayIterator<>(array, 0, array.length - 2);

        Object[][] itered = new Object[8][];
        int i = 0;
        while (iter.hasNext()) {
            itered[i] = iter.next();
            i++;
        }
        assertThat(itered, is(arrayContaining(Arrays.copyOf(array, array.length-2))));
    }

    @Test
    public void testIterMore() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("end exceeds array length");

        Object[][] array = createArray(10);
        new ArrayIterator<>(array, 0, array.length + 2);
    }

    @Test
    public void testEmptyArray() throws Exception {
        Object[][] array = createArray(0);
        ArrayIterator<Object[]> iter = new ArrayIterator<>(array, 0, 0);
        assertThat(iter.hasNext(), is(false));
    }

    @Test
    public void testRewind() throws Exception {
        Object[][] array = createArray(10);
        ArrayIterator<Object[]> iter = new ArrayIterator<>(array, 0, array.length - 2);

        Object[][] itered = new Object[10][];
        int i = 0;
        while (iter.hasNext()) {
            itered[i] = iter.next();
            i++;
        }
        assertThat(iter.rewind(2), is(2));
        while (iter.hasNext()) {
            itered[i] = iter.next();
            i++;
        }
        assertThat(itered.length, is(10));
        assertThat(itered[6], is(itered[8]));
        assertThat(itered[7], is(itered[9]));
    }

    @Test
    public void testRewindMore() throws Exception {
        Object[][] array = createArray(10);
        ArrayIterator<Object[]> iter = new ArrayIterator<>(array, 0, array.length);

        Object[][] itered = new Object[20][];
        int i = 0;
        while (iter.hasNext()) {
            itered[i] = iter.next();
            i++;
        }

        assertThat(iter.rewind(12), is(10));
        assertThat(iter.hasNext(), is(true));
        while (iter.hasNext()) {
            itered[i] = iter.next();
            i++;
        }
        assertThat(itered.length, is(20));
        assertThat(Arrays.copyOfRange(itered, 0, 10), is(array));
        assertThat(Arrays.copyOfRange(itered, 10, 20), is(array));
    }
}
