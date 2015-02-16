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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import io.crate.core.bigarray.IterableBigArray;
import io.crate.core.bigarray.MultiNativeArrayBigArray;
import org.elasticsearch.common.util.ObjectArray;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.range;
import static org.hamcrest.core.Is.is;

public class FetchedRowsPageableTaskResultTest extends RandomizedTest{

    private void assertPageEquals(Page page, Object[][] pageSource, int from, int size) {
        int actualLength = pageSource.length - from;
        assertThat((int)page.size(), is(Math.min(actualLength, size)));
        for (Object[] row : page) {
            assertThat(row, is(pageSource[from]));
            from++;
        }
    }

    private void assertPageEquals(Page page, ObjectArray<Object[]> pageSource, int from, int size) {
        long actualLength = pageSource.size() - from;
        assertThat(page.size(), is(Math.min(actualLength, size)));
        for (Object[] row : page) {
            assertThat(row, is(pageSource.get(from)));
            from++;
        }
    }

    @Test
    public void testFetchedRowsObjectArray() throws Exception {
        Object[][] array = range(0, 100);
        int pageSize = randomIntBetween(1, 100);
        PageInfo pageInfo = PageInfo.firstPage(pageSize);
        TaskResult pageableTaskResult = FetchedRowsPageableTaskResult.forArray(array, 0, pageInfo);

        while (pageableTaskResult.page().size() > 0) {
            assertPageEquals(pageableTaskResult.page(), array, pageInfo.position(), pageSize);

            pageInfo = pageInfo.nextPage();
            pageableTaskResult = pageableTaskResult.fetch(pageInfo).get();
        }
    }

    @Test
    public void testFetchedRowsBigArray() throws Exception {
        IterableBigArray<Object[]> array = new MultiNativeArrayBigArray<Object[]>(0, 100, range(0, 100));
        int pageSize = randomIntBetween(1, 100);
        PageInfo pageInfo = PageInfo.firstPage(pageSize);
        TaskResult pageableTaskResult = FetchedRowsPageableTaskResult.forArray(array, 0, pageInfo);

        while (pageableTaskResult.page().size() > 0) {
            assertPageEquals(pageableTaskResult.page(), array, pageInfo.position(), pageSize);

            pageInfo = pageInfo.nextPage();
            pageableTaskResult = pageableTaskResult.fetch(pageInfo).get();
        }
    }

    @Test
    public void testFetchedRowsEmptyObjectArray() throws Exception {
        Object[][] array = range(0, 0);
        int pageSize = randomIntBetween(1, 100);
        PageInfo pageInfo = PageInfo.firstPage(pageSize);
        TaskResult pageableTaskResult = FetchedRowsPageableTaskResult.forArray(array, 0, pageInfo);

        while (pageableTaskResult.page().size() > 0) {
            assertThat(pageableTaskResult.page().size(), is(0L));

            pageInfo = pageInfo.nextPage();
            pageableTaskResult = pageableTaskResult.fetch(pageInfo).get();
        }
    }
}
