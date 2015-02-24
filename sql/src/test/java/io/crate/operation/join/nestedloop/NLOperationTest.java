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

package io.crate.operation.join.nestedloop;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.google.common.collect.FluentIterable;
import io.crate.executor.ObjectArrayPage;
import io.crate.executor.Page;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.testing.TestingHelpers;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;


public class NLOperationTest extends RandomizedTest {

    private Observable<Page> inner;
    private Observable<Page> outer;
    private String outerXInnerOutput;

    @Before
    public void setUp() throws Exception {
        Page outerPage1 = new ObjectArrayPage(new Object[][]{
                new Object[]{"Green"},
                new Object[]{"Blue"}
        });
        Page outerPage2 = new ObjectArrayPage(new Object[][]{
                new Object[]{"Red"},
        });
        outer = Observable.just(outerPage1, outerPage2);

        Page innerPage1 = new ObjectArrayPage(new Object[][] {
                new Object[] { "small" },
                new Object[] { "medium" }
        });
        Page innerPage2 = new ObjectArrayPage(new Object[][]{
                new Object[]{"large"},
                new Object[]{"very large"}
        });
        Page innerPage3 = new ObjectArrayPage(new Object[][]{
                new Object[]{"very very large"}
        });
        inner = Observable.just(innerPage1, innerPage2, innerPage3);

        outerXInnerOutput = "Green| small\n" +
                "Green| medium\n" +
                "Green| large\n" +
                "Green| very large\n" +
                "Green| very very large\n" +
                "Blue| small\n" +
                "Blue| medium\n" +
                "Blue| large\n" +
                "Blue| very large\n" +
                "Blue| very very large\n" +
                "Red| small\n" +
                "Red| medium\n" +
                "Red| large\n" +
                "Red| very large\n" +
                "Red| very very large\n";
    }

    @Test
    public void testEmptyOuter() throws Exception {
        NLOperation nlOperation = new NLOperation(Observable.<Page>empty(), inner);
        List<Object[]> rows = consumeRows(nlOperation.execute(0, 10, 100));
        assertThat(rows.size(), is(0));
    }

    @Test
    public void testEmptyInner() throws Exception {
        NLOperation nlOperation = new NLOperation(outer, Observable.<Page>empty());
        List<Object[]> rows = consumeRows(nlOperation.execute(0, 10, 100));
        assertThat(rows.size(), is(0));
    }

    @Test
    public void testNLOperationWithDownstream() throws Exception {
        NLOperation nlOperation = new NLOperation(outer, inner);

        CollectingProjector collectingProjector = new CollectingProjector();
        nlOperation.execute(collectingProjector);

        Object[][] rows = collectingProjector.result().get();
        assertThat(TestingHelpers.printedTable(rows), is(outerXInnerOutput));
    }

    @Test
    public void testNestedNestedLoopOperation() throws Exception {
        NLOperation innerNLOperation = new NLOperation(outer, inner);
        Page page = new ObjectArrayPage(new Object[][] {
                new Object[] { "Arthur" },
                new Object[] { "Trillian" }
        });
        NLOperation nlOperation = new NLOperation(innerNLOperation.execute(0, 100, 2), Observable.just(page));
        List<Object[]> rows = consumeRows(nlOperation.execute(0, 100, 2));
        assertThat(
                TestingHelpers.printedTable(rows.toArray(new Object[rows.size()][])),
                is("Green| small| Arthur\n" +
                        "Green| small| Trillian\n" +
                        "Green| medium| Arthur\n" +
                        "Green| medium| Trillian\n" +
                        "Green| large| Arthur\n" +
                        "Green| large| Trillian\n" +
                        "Green| very large| Arthur\n" +
                        "Green| very large| Trillian\n" +
                        "Green| very very large| Arthur\n" +
                        "Green| very very large| Trillian\n" +
                        "Blue| small| Arthur\n" +
                        "Blue| small| Trillian\n" +
                        "Blue| medium| Arthur\n" +
                        "Blue| medium| Trillian\n" +
                        "Blue| large| Arthur\n" +
                        "Blue| large| Trillian\n" +
                        "Blue| very large| Arthur\n" +
                        "Blue| very large| Trillian\n" +
                        "Blue| very very large| Arthur\n" +
                        "Blue| very very large| Trillian\n" +
                        "Red| small| Arthur\n" +
                        "Red| small| Trillian\n" +
                        "Red| medium| Arthur\n" +
                        "Red| medium| Trillian\n" +
                        "Red| large| Arthur\n" +
                        "Red| large| Trillian\n" +
                        "Red| very large| Arthur\n" +
                        "Red| very large| Trillian\n" +
                        "Red| very very large| Arthur\n" +
                        "Red| very very large| Trillian\n")
        );
    }

    @Test
    public void testConsumeOnlyFirstPage() throws Exception {
        NLOperation nlOperation = new NLOperation(outer, inner);
        Observable<Page> result = nlOperation.execute(0, 100, 10);

        Observable<Page> take = result.take(1);
        final List<Object[]> rows = new ArrayList<>();
        take.forEach(new Action1<Page>() {
            @Override
            public void call(Page page) {
                for (Object[] row : page) {
                    rows.add(row);
                }
            }
        });
        assertThat(rows.size(), is(10));
    }

    @Test
    public void testConsumeAll() throws Exception {
        NLOperation nlOperation = new NLOperation(outer, inner);
        Observable<Page> result = nlOperation.execute(0, 100, 3);
        List<Object[]> rows = consumeRows(result);
        assertThat(rows.size(), is(15));

        assertThat(TestingHelpers.printedTable(rows.toArray(new Object[rows.size()][])), is(outerXInnerOutput));
    }

    @Test
    public void testConsumeAllWithOffset() throws Exception {
        List<Object[]> rows = consumeRows(new NLOperation(outer, inner).execute(10, 100, 100));
        assertThat(rows.size(), is(5));
        assertThat(TestingHelpers.printedTable(rows.toArray(new Object[rows.size()][])),
                is("Red| small\n" +
                "Red| medium\n" +
                "Red| large\n" +
                "Red| very large\n" +
                "Red| very very large\n"));
    }

    @Test
    public void testPageSizeIsCorrect() throws Throwable {
        Observable<Page> observable = new NLOperation(outer, inner).execute(0, 100, 2);
        final AtomicInteger pages = new AtomicInteger(0);
        final AtomicReference<Throwable> lastException = new AtomicReference<>();
        observable.subscribe(new Action1<Page>() {
            @Override
            public void call(Page objects) {
                try {
                    assertThat(objects.size(), Matchers.anyOf(is(2L), is(1L)));
                } catch (Throwable e) {
                    lastException.set(e);
                }
                pages.incrementAndGet();
            }
        });
        Throwable throwable = lastException.get();
        if (throwable != null) {
            throw throwable;
        }
        assertThat(pages.get(), is(8));
    }

    private List<Object[]> consumeRows(Observable<Page> result) {
        final List<Object[]> rows = new ArrayList<>();
        result.forEach(new Action1<Page>() {
            @Override
            public void call(Page page) {
                for (Object[] row : page) {
                    rows.add(row);
                }
            }
        });
        return rows;
    }

    @Test
    public void testManyAndLargePages() throws Exception {
        Object[][] rows = new Object[10_000][];
        Arrays.fill(rows, new Object[] { "foo", "bar" });
        ObjectArrayPage page = new ObjectArrayPage(rows);

        FluentIterable<Page> fluentIterable = FluentIterable.of(new Page[] { page });
        for (int i = 0; i < 20; i++) {
            fluentIterable = fluentIterable.append(page);
        }
        Observable<Page> outer = Observable.from(fluentIterable);

        fluentIterable = FluentIterable.of(new Page[] { page, page, page });
        Observable<Page> inner = Observable.from(fluentIterable);
        NLOperation nlOperation = new NLOperation(outer, inner);

        Observable<Page> result = nlOperation.execute(0, 15_000, 3000).take(5);
        List<Object[]> objects = consumeRows(result);
        assertThat(objects.size(), is(15_000));
    }
}