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

package io.crate.core.bigarray;

import com.google.common.collect.Iterators;
import io.crate.core.collections.RewindableIterator;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Iterator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class NativeObjectArrayBigArrayTest {

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
    public void testSingleArrayZeroSize() throws Exception {
        Object[][] backingArray = createArray(10);

        MultiNativeArrayBigArray<Object[]> bigArray = new MultiNativeArrayBigArray<Object[]>(0, 0, backingArray);
        assertThat(bigArray.size(), is(0L));

        expectedException.expect(ArrayIndexOutOfBoundsException.class);
        expectedException.expectMessage("index 0 exceeds bounds of backing arrays");

        bigArray.get(0L);
    }

    @Test
    public void testSingleArrayOffsetExceedsBound() throws Exception {
        Object[][] backingArray = createArray(10);
        MultiNativeArrayBigArray<Object[]> bigArray = new MultiNativeArrayBigArray<Object[]>(10, 0, backingArray);
        assertThat(bigArray.size(), is(0L));

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("offset exceeds backing arrays");

        new MultiNativeArrayBigArray<Object[]>(11, 1, backingArray);
    }

    @Test
    public void testSingleArrayZeroSizeWithBigOffset() throws Exception {
        Object[][] backingArray = createArray(10);
        MultiNativeArrayBigArray<Object[]> bigArray = new MultiNativeArrayBigArray<Object[]>(10, 0, backingArray);
        assertThat(bigArray.size(), is(0L));
    }

    @Test
    public void testSingleArraySizeOne() throws Exception {
        Object[][] backingArray = createArray(10);
        MultiNativeArrayBigArray<Object[]> bigArray = new MultiNativeArrayBigArray<Object[]>(9, 1, backingArray);
        assertThat(bigArray.size(), is(1L));

        Object[] row = bigArray.get(0L);
        assertThat(row, Matchers.<Object>arrayContaining(9));

        expectedException.expect(ArrayIndexOutOfBoundsException.class);
        expectedException.expectMessage("index 1 exceeds bounds of backing arrays");

        bigArray.get(1L);
    }

    @Test
    public void testSingleArraySmallSize() throws Exception {
        Object[][] backingArray = createArray(10);
        MultiNativeArrayBigArray<Object[]> bigArray = new MultiNativeArrayBigArray<Object[]>(0, 5, backingArray);
        assertThat(bigArray.size(), is(5L));

        Object[] row = bigArray.get(0L);
        assertThat(row, Matchers.<Object>arrayContaining(0));

        int i = 0;
        for (Object[] element : bigArray) {
            assertThat(element[0], Matchers.<Object>is(i));
            i++;
        }
        assertThat(i, is(5));

    }

    @Test
    public void testNegativeIndex() throws Exception {
        Object[][] backingArray = createArray(10);
        MultiNativeArrayBigArray<Object[]> bigArray = new MultiNativeArrayBigArray<Object[]>(0, 10, backingArray);
        assertThat(bigArray.size(), is(10L));

        expectedException.expect(ArrayIndexOutOfBoundsException.class);
        expectedException.expectMessage("index -19 exceeds bounds of backing arrays");

        bigArray.get(-19L);
    }

    @Test
    public void testMultipleArrays() throws Exception {
        Object[][] backingArray1 = createArray(7);
        Object[][] backingArray2 = createArray(10);
        MultiNativeArrayBigArray<Object[]> bigArray = new MultiNativeArrayBigArray<>(0, 17, backingArray1, backingArray2);
        assertThat(bigArray.size(), is(17L));
        int i = 0;
        for (; i < 7; i++) {
            assertThat(bigArray.get(i), is(Matchers.<Object>arrayContaining(i)));
        }
        int j = 0;
        for (; i < 17; i++) {
            assertThat(bigArray.get(i), is(Matchers.<Object>arrayContaining(j++)));
        }

        expectedException.expect(ArrayIndexOutOfBoundsException.class);
        expectedException.expectMessage("index 17 exceeds bounds of backing arrays");
        bigArray.get(i);
    }

    @Test
    public void testNullSizeArray() throws Exception {
        Object[][] backingArray1 = createArray(1);
        Object[][] backingArray2 = createArray(0);
        Object[][] backingArray3 = createArray(9);
        MultiNativeArrayBigArray<Object[]> bigArray = new MultiNativeArrayBigArray<>(
                0, 10,
                backingArray1,
                backingArray2,
                backingArray3);
        assertThat(bigArray.size(), is(10L));
        assertThat(bigArray.get(0L), is(Matchers.<Object>arrayContaining(0)));

        for (int i=1; i < 9; i++) {
            assertThat(bigArray.get(i), is(Matchers.<Object>arrayContaining(i-1)));
        }

        Object[] iterResults = new Object[(int)bigArray.size()];
        int iter = 0;
        for (Object[] row : bigArray) {
            iterResults[iter++] = row[0];
        }
        assertThat(iterResults, is(Matchers.<Object>arrayContaining(0, 0, 1, 2, 3, 4, 5, 6, 7, 8)));

        bigArray = new MultiNativeArrayBigArray<>(
                1, 9,
                backingArray1,
                backingArray2,
                backingArray3);
        assertThat(bigArray.size(), is(9L));
        for (int i=0; i < 9; i++) {
            assertThat(bigArray.get(i), is(Matchers.<Object>arrayContaining(i)));
        }

        iterResults = new Object[(int)bigArray.size()];
        int iter2 = 0;
        for (Object[] row : bigArray) {
            iterResults[iter2++] = row[0];
        }
        assertThat(iterResults, is(Matchers.<Object>arrayContaining(0, 1, 2, 3, 4, 5, 6, 7, 8)));

    }

    @Test
    public void testSet() throws Exception {
        Object[][] backingArray1 = createArray(1);
        Object[][] backingArray2 = createArray(0);
        Object[][] backingArray3 = createArray(9);
        MultiNativeArrayBigArray<Object[]> bigArray = new MultiNativeArrayBigArray<>(
                0, 10,
                backingArray1,
                backingArray2,
                backingArray3);
        assertThat(bigArray.size(), is(10L));
        assertThat(bigArray.get(0L), is(Matchers.<Object>arrayContaining(0)));

        Object[] empty = new Object[0];
        for (int i=1; i < 9; i++) {
            assertThat(bigArray.set(i, empty), is(Matchers.<Object>arrayContaining(i-1)));
        }
        for (int i=1; i < 9; i++) {
            assertThat(bigArray.get(i), is(empty));
        }
    }

    @Test
    public void testIterator() throws Exception {
        Object[][] backingArray = createArray(10);
        MultiNativeArrayBigArray<Object[]> bigArray = new MultiNativeArrayBigArray<Object[]>(0, 10, backingArray);
        int i = 0;
        for (Object[] row : bigArray) {
            assertThat(row, is(Matchers.<Object>arrayContaining(i++)));
        }
        assertThat(i, is(10));

        bigArray = new MultiNativeArrayBigArray<Object[]>(5, 5, backingArray);

        i = 5;
        for (Object[] row : bigArray) {
            assertThat(row, is(Matchers.<Object>arrayContaining(i++)));
        }
        assertThat(i, is(10));

        bigArray = new MultiNativeArrayBigArray<Object[]>(5, 2, backingArray);

        i = 5;
        for (Object[] row : bigArray) {
            assertThat(row, is(Matchers.<Object>arrayContaining(i++)));
        }
        assertThat(i, is(7));

        bigArray = new MultiNativeArrayBigArray<Object[]>(0, 0, new Object[0][]);
        assertThat(bigArray.iterator().hasNext(), is(false));

        bigArray = new MultiNativeArrayBigArray<>(0, 20, new Object[0][], backingArray, backingArray);
        i = 0;
        for (Object[] row : bigArray) {
            assertThat(row, is(Matchers.<Object>arrayContaining(i%10)));
            i++;
        }
        assertThat(i, is(20));

        Iterator<Object[]> iter = bigArray.iterator();
        assertThat(iter, instanceOf(RewindableIterator.class));
        RewindableIterator<Object[]> rewindIter = (RewindableIterator<Object[]>)iter;

        i = 0;
        while (rewindIter.hasNext()) {
            rewindIter.next();
            i++;
        }
        assertThat(i, is(20));

        assertThat(rewindIter.rewind(0), is(0));
        assertThat(rewindIter.hasNext(), is(false));
        assertThat(rewindIter.rewind(1), is(1));
        assertThat(rewindIter.hasNext(), is(true));
        Object[] lastRow = rewindIter.next();

        assertThat(rewindIter.hasNext(), is(false));
        assertThat(rewindIter.rewind(4), is(4));
        assertThat(rewindIter.hasNext(), is(true));
        assertThat(Iterators.advance(rewindIter, 3), is(3));
        assertThat(rewindIter.next(), is(lastRow));

    }

}
