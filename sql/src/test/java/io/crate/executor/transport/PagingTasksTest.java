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

package io.crate.executor.transport;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.action.sql.SQLResponse;
import io.crate.analyze.WhereClause;
import io.crate.executor.*;
import io.crate.executor.transport.task.elasticsearch.QueryThenFetchTask;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.node.dql.QueryThenFetchNode;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class PagingTasksTest extends BaseTransportExecutorTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private Closeable closeMeWhenDone;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @After
    public void freeResources() throws Exception {
        if (closeMeWhenDone != null) {
            closeMeWhenDone.close();
        }
    }

    @Test
    public void testPagedQueryThenFetch() throws Exception {
        setup.setUpCharacters();
        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        QueryThenFetchNode qtfNode = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(idRef, nameRef, femaleRef),
                Arrays.<Symbol>asList(nameRef, idRef),
                new boolean[]{false, false},
                new Boolean[]{null, null},
                5,
                0,
                WhereClause.MATCH_ALL,
                null
        );

        List<Task> tasks = executor.newTasks(qtfNode, UUID.randomUUID());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = PageInfo.firstPage(2);

        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));
        ListenableFuture<TaskResult> resultFuture = results.get(0);

        TaskResult result = resultFuture.get();
        assertThat(result, instanceOf(PageableTaskResult.class));
        PageableTaskResult pageableResult = (PageableTaskResult)result;
        assertThat(TestingHelpers.printedPage(pageableResult.page()), is(
                "1| Arthur| false\n" +
                "4| Arthur| true\n"
        ));
        pageInfo = pageInfo.nextPage(2);
        ListenableFuture<PageableTaskResult> nextPageResultFuture = ((PageableTaskResult)result).fetch(pageInfo);
        PageableTaskResult nextPageResult = nextPageResultFuture.get();
        closeMeWhenDone = nextPageResult;
        assertThat(TestingHelpers.printedPage(nextPageResult.page()), is(
                "2| Ford| false\n" +
                "3| Trillian| true\n"
        ));

        pageInfo = pageInfo.nextPage(2);
        Page nextPage = nextPageResult.fetch(pageInfo).get().page();
        assertThat(nextPage.size(), is(0L));

    }

    @Test
    public void testPagedQueryThenFetchWithOffset() throws Exception {
        setup.setUpCharacters();
        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        QueryThenFetchNode qtfNode = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(idRef, nameRef, femaleRef),
                Arrays.<Symbol>asList(nameRef, idRef),
                new boolean[]{false, false},
                new Boolean[]{null, null},
                5,
                0,
                WhereClause.MATCH_ALL,
                null
        );

        List<Task> tasks = executor.newTasks(qtfNode, UUID.randomUUID());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = new PageInfo(1, 2);
        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));
        ListenableFuture<TaskResult> resultFuture = results.get(0);
        TaskResult result = resultFuture.get();
        assertThat(result, instanceOf(PageableTaskResult.class));
        PageableTaskResult pageableResult = (PageableTaskResult)result;
        closeMeWhenDone = pageableResult;
        assertThat(TestingHelpers.printedPage(pageableResult.page()), is(
                "4| Arthur| true\n" +
                "2| Ford| false\n"
        ));
        pageInfo = pageInfo.nextPage(2);
        ListenableFuture<PageableTaskResult> nextPageResultFuture = ((PageableTaskResult)result).fetch(pageInfo);
        PageableTaskResult nextPageResult = nextPageResultFuture.get();
        closeMeWhenDone = nextPageResult;
        assertThat(TestingHelpers.printedPage(nextPageResult.page()), is(
                "3| Trillian| true\n"
        ));

        pageInfo = pageInfo.nextPage(2);
        Page lastPage = nextPageResult.fetch(pageInfo).get().page();
        assertThat(lastPage.size(), is(0L));
    }

    @Test
    public void testPagedQueryThenFetchWithoutSorting() throws Exception {
        setup.setUpCharacters();
        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        QueryThenFetchNode qtfNode = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(idRef, nameRef, femaleRef),
                null,
                null,
                null,
                5,
                0,
                WhereClause.MATCH_ALL,
                null
        );

        List<Task> tasks = executor.newTasks(qtfNode, UUID.randomUUID());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = new PageInfo(1, 2);
        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));

        ListenableFuture<TaskResult> resultFuture = results.get(0);
        TaskResult result = resultFuture.get();
        assertThat(result, instanceOf(PageableTaskResult.class));
        closeMeWhenDone = (PageableTaskResult)result;
        PageableTaskResult firstResult = (PageableTaskResult)result;
        assertThat(firstResult.page().size(), is(2L));

        pageInfo = pageInfo.nextPage(2);
        ListenableFuture<PageableTaskResult> nextPageResultFuture = ((PageableTaskResult)result).fetch(pageInfo);
        PageableTaskResult nextPageResult = nextPageResultFuture.get();
        closeMeWhenDone = nextPageResult;
        assertThat(nextPageResult.page().size(), is(1L));

        pageInfo = pageInfo.nextPage(2);
        PageableTaskResult furtherPageResult = nextPageResult.fetch(pageInfo).get();
        closeMeWhenDone = furtherPageResult;
        assertThat(furtherPageResult.page().size(), is(0L));
    }

    @Test
    public void testPagedQueryThenFetch1RowPages() throws Exception {
        setup.setUpCharacters();
        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        QueryThenFetchNode qtfNode = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(idRef, nameRef, femaleRef),
                null,
                null,
                null,
                null,
                null,
                WhereClause.MATCH_ALL,
                null
        );

        List<Task> tasks = executor.newTasks(qtfNode, UUID.randomUUID());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = new PageInfo(1, 1);
        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));

        // first page
        ListenableFuture<TaskResult> resultFuture = results.get(0);
        TaskResult result = resultFuture.get();
        assertThat(result, instanceOf(PageableTaskResult.class));
        PageableTaskResult pageableResult = (PageableTaskResult)result;
        closeMeWhenDone = pageableResult;
        assertThat(pageableResult.page().size(), is(1L));

        PageableTaskResult nextPageResult = (PageableTaskResult)result;
        for (int i = 0; i<2 ; i++) {
            pageInfo = pageInfo.nextPage();
            ListenableFuture<PageableTaskResult> nextPageResultFuture = nextPageResult.fetch(pageInfo);
            nextPageResult = nextPageResultFuture.get();
            closeMeWhenDone = nextPageResult;
            assertThat(nextPageResult.page().size(), is(1L));
        }
        // no further pages
        assertThat(nextPageResult.fetch(pageInfo.nextPage()).get().page().size(), is(0L));
    }

    private Reference ref(TableInfo tableInfo, String colName) {
        return new Reference(tableInfo.getReferenceInfo(ColumnIdent.fromPath(colName)));
    }

    @Test
    public void testPartitionedPagedQueryThenFetch1RowPages() throws Exception {
        setup.setUpPartitionedTableWithName();
        DocTableInfo parted = docSchemaInfo.getTableInfo("parted");

        QueryThenFetchNode qtfNode = new QueryThenFetchNode(
                parted.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(ref(parted, "id"), ref(parted, "name"), ref(parted, "date")),
                Arrays.<Symbol>asList(ref(parted, "id")),
                new boolean[]{false},
                new Boolean[]{false},
                null,
                null,
                WhereClause.MATCH_ALL,
                parted.partitionedByColumns()
        );

        List<Task> tasks = executor.newTasks(qtfNode, UUID.randomUUID());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = new PageInfo(0, 1);
        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));

        // first page
        ListenableFuture<TaskResult> resultFuture = results.get(0);
        TaskResult result = resultFuture.get();
        assertThat(result, instanceOf(PageableTaskResult.class));
        PageableTaskResult pageableResult = (PageableTaskResult)result;
        closeMeWhenDone = pageableResult;
        assertThat(TestingHelpers.printedPage(pageableResult.page()), is("1| Trillian| NULL\n"));

        List<String> pages = new ArrayList<>();

        PageableTaskResult nextPageResult = (PageableTaskResult)result;
        while (nextPageResult.page().size() > 0) {
            pageInfo = pageInfo.nextPage();
            ListenableFuture<PageableTaskResult> nextPageResultFuture = nextPageResult.fetch(pageInfo);
            nextPageResult = nextPageResultFuture.get();
            closeMeWhenDone = nextPageResult;
            pages.add(TestingHelpers.printedPage(nextPageResult.page()));
        }

        assertThat(Joiner.on("").join(pages), is(
                "2| NULL| 0\n" +
                "3| Ford| 1396388720242\n"));
        // no further pages
        assertThat(nextPageResult.page().size(), is(0L));
    }

    @Test
    public void testPagedQueryThenFetchWithQueryOffset() throws Exception {
        setup.setUpCharacters();
        sqlExecutor.exec("insert into characters (id, name, female) values (?, ?, ?)", new Object[][]{
                new Object[]{
                        5, "Matthias Wahl", false,
                },
                new Object[]{
                        6, "Philipp Bogensberger", false,

                },
                new Object[]{
                        7, "Sebastian Utz", false
                }
        });
        sqlExecutor.refresh("characters");

        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        QueryThenFetchNode qtfNode = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(idRef, nameRef, femaleRef),
                ImmutableList.<Symbol>of(idRef),
                new boolean[]{ false },
                new Boolean[]{ null },
                null,
                1,
                WhereClause.MATCH_ALL,
                null
        );

        List<Task> tasks = executor.newTasks(qtfNode, UUID.randomUUID());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = new PageInfo(1, 1);
        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));

        // first page
        ListenableFuture<TaskResult> resultFuture = results.get(0);
        TaskResult result = resultFuture.get();
        assertThat(result, instanceOf(PageableTaskResult.class));
        PageableTaskResult pageableResult = (PageableTaskResult)result;
        closeMeWhenDone = pageableResult;

        // the two first records were hit by the query and page offset
        assertThat(TestingHelpers.printedPage(pageableResult.page()), is(
                "3| Trillian| true\n"));

        pageInfo = pageInfo.nextPage(1);
        ListenableFuture<PageableTaskResult> nextPageResultFuture = pageableResult.fetch(pageInfo);
        PageableTaskResult nextPageResult = nextPageResultFuture.get();
        closeMeWhenDone = nextPageResult;
        assertThat(TestingHelpers.printedPage(nextPageResult.page()), is(
                "4| Arthur| true\n"));

        pageInfo = pageInfo.nextPage(5);
        nextPageResultFuture = nextPageResult.fetch(pageInfo);
        nextPageResult = nextPageResultFuture.get();
        closeMeWhenDone = nextPageResult;
        assertThat(TestingHelpers.printedPage(nextPageResult.page()), is(
                "5| Matthias Wahl| false\n" +
                "6| Philipp Bogensberger| false\n" +
                "7| Sebastian Utz| false\n"));

        // no further pages
        assertThat(nextPageResult.fetch(pageInfo.nextPage()).get().page().size(), is(0L));
    }

    @Test
    public void testQueryThenFetchPageWithGap() throws Exception {
        setup.setUpCharacters();
        sqlExecutor.exec("insert into characters (id, name, female) values (?, ?, ?)", new Object[][]{
                new Object[]{
                        5, "Matthias Wahl", false,
                },
                new Object[]{
                        6, "Philipp Bogensberger", false,

                },
                new Object[]{
                        7, "Sebastian Utz", false
                }
        });
        sqlExecutor.refresh("characters");

        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        QueryThenFetchNode qtfNode = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(idRef, nameRef, femaleRef),
                Arrays.<Symbol>asList(idRef),
                new boolean[]{false},
                new Boolean[]{null},
                null,
                null,
                WhereClause.MATCH_ALL,
                null
        );

        List<Task> tasks = executor.newTasks(qtfNode, UUID.randomUUID());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = PageInfo.firstPage(2);

        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));
        ListenableFuture<TaskResult> resultFuture = results.get(0);

        TaskResult taskResult = resultFuture.get();
        assertThat(taskResult, instanceOf(PageableTaskResult.class));
        PageableTaskResult pageableTaskResult = (PageableTaskResult)taskResult;
        closeMeWhenDone = pageableTaskResult;
        assertThat(TestingHelpers.printedPage(pageableTaskResult.page()),
                is("1| Arthur| false\n" +
                   "2| Ford| false\n")
        );

        pageInfo = new PageInfo(4, 2);
        PageableTaskResult gappedResult = pageableTaskResult.fetch(pageInfo).get();
        closeMeWhenDone = gappedResult;
        assertThat(TestingHelpers.printedPage(gappedResult.page()),
                is("5| Matthias Wahl| false\n" +
                   "6| Philipp Bogensberger| false\n")
        );

    }

    @Test
    public void testQueryThenFetchPageWithBigGap() throws Exception {
        sqlExecutor.exec("create table ids (id long primary key) with (number_of_replicas=0)");
        sqlExecutor.ensureGreen();
        Object[][] bulkArgs = new Object[2048][];
        for (int l = 0; l<2048; l++) {
            bulkArgs[l] = new Object[] { l };
        }
        sqlExecutor.exec("insert into ids values (?)", bulkArgs);
        sqlExecutor.refresh("ids");
        assertThat(
                (Long)sqlExecutor.exec("select count(*) from ids").rows()[0][0],
                is(2048L)
        );

        DocTableInfo ids = docSchemaInfo.getTableInfo("ids");

        QueryThenFetchNode qtfNode = new QueryThenFetchNode(
                ids.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(ref(ids, "id")),
                Arrays.<Symbol>asList(ref(ids, "id")),
                new boolean[]{false},
                new Boolean[]{null},
                null,
                null,
                WhereClause.MATCH_ALL,
                null
        );

        List<Task> tasks = executor.newTasks(qtfNode, UUID.randomUUID());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = PageInfo.firstPage(2);

        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));
        ListenableFuture<TaskResult> resultFuture = results.get(0);

        TaskResult taskResult = resultFuture.get();
        assertThat(taskResult, instanceOf(PageableTaskResult.class));
        PageableTaskResult pageableTaskResult = (PageableTaskResult)taskResult;
        closeMeWhenDone = pageableTaskResult;
        assertThat(TestingHelpers.printedPage(pageableTaskResult.page()),
                is("0\n" +
                   "1\n")
        );

        pageInfo = new PageInfo(1048, 4);
        PageableTaskResult gappedResult = pageableTaskResult.fetch(pageInfo).get();
        closeMeWhenDone = gappedResult;
        assertThat(TestingHelpers.printedPage(gappedResult.page()),
                is("1048\n" +
                   "1049\n" +
                   "1050\n" +
                   "1051\n")
        );

        pageInfo = pageInfo.nextPage(10);
        PageableTaskResult afterGappedResult = gappedResult.fetch(pageInfo).get();
        closeMeWhenDone = afterGappedResult;
        assertThat(TestingHelpers.printedPage(afterGappedResult.page()),
                is("1052\n" +
                   "1053\n" +
                   "1054\n" +
                   "1055\n" +
                   "1056\n" +
                   "1057\n" +
                   "1058\n" +
                   "1059\n" +
                   "1060\n" +
                   "1061\n")
        );

    }

    @TestLogging("io.crate.executor.transport.task.elasticsearch:TRACE")
    @Test
    public void testRandomQTFPaging() throws Exception {
        SQLResponse response = sqlExecutor.exec("create table ids (id long primary key) with (number_of_replicas=0)");
        assertThat(response.rowCount(), is(1L));
        sqlExecutor.ensureGreen();

        int numRows = randomIntBetween(1, 4096);
        Object[][] bulkArgs = new Object[numRows][];
        for (int l = 0; l < numRows; l++) {
            bulkArgs[l] = new Object[]{ (long)l};
        }
        sqlExecutor.exec("insert into ids values (?)", bulkArgs);
        sqlExecutor.refresh("ids");
        assertThat(
                (Long) sqlExecutor.exec("select count(*) from ids").rows()[0][0],
                is((long)numRows)
        );

        DocTableInfo ids = docSchemaInfo.getTableInfo("ids");

        QueryThenFetchNode qtfNode = new QueryThenFetchNode(
                ids.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(ref(ids, "id")),
                Arrays.<Symbol>asList(ref(ids, "id")),
                new boolean[]{false},
                new Boolean[]{null},
                null,
                null,
                WhereClause.MATCH_ALL,
                null
        );

        List<Task> tasks = executor.newTasks(qtfNode, UUID.randomUUID());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = new PageInfo(
                randomIntBetween(0, 100),
                randomIntBetween(1, 100));

        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));
        ListenableFuture<TaskResult> resultFuture = results.get(0);

        TaskResult taskResult = resultFuture.get();
        assertThat(taskResult, instanceOf(PageableTaskResult.class));
        PageableTaskResult pageableTaskResult = (PageableTaskResult)taskResult;
        closeMeWhenDone = pageableTaskResult;

        int run = 0;
        while (run < 10) {
            Page page = pageableTaskResult.page();
            Object[][] actual = Iterables.toArray(page, Object[].class);
            Object[][] expected = Arrays.copyOfRange(bulkArgs, Math.min(numRows, pageInfo.position()), Math.min(numRows, pageInfo.position() + pageInfo.size()));

            for (int i = 0 ; i<expected.length && i < actual.length; i++) {
                assertThat(
                        String.format("elements at index %d differ for page (%s) %d", i, pageInfo, run),
                        actual[i], is(expected[i]));
                i++;
            }
            assertThat("actual and expected results differ in length", actual.length, is(expected.length));
            pageInfo = new PageInfo(
                    randomIntBetween(0, numRows),
                    randomIntBetween(1, numRows)
            );
            pageableTaskResult = pageableTaskResult.fetch(pageInfo).get();
            closeMeWhenDone = pageableTaskResult;
            run++;
        }
    }

    @Test
    public void testQueryThenFetchRepeatPage() throws Exception {
        setup.setUpCharacters();
        sqlExecutor.exec("insert into characters (id, name, female) values (?, ?, ?)", new Object[][]{
                new Object[]{
                        5, "Matthias Wahl", false,
                },
                new Object[]{
                        6, "Philipp Bogensberger", false,
                },
                new Object[]{
                        7, "Sebastian Utz", false
                }
        });
        sqlExecutor.refresh("characters");

        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        QueryThenFetchNode qtfNode = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(idRef, nameRef, femaleRef),
                Arrays.<Symbol>asList(idRef),
                new boolean[]{false},
                new Boolean[]{null},
                null,
                null,
                WhereClause.MATCH_ALL,
                null
        );

        List<Task> tasks = executor.newTasks(qtfNode, UUID.randomUUID());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = PageInfo.firstPage(2);

        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));
        ListenableFuture<TaskResult> resultFuture = results.get(0);

        TaskResult taskResult = resultFuture.get();
        assertThat(taskResult, instanceOf(PageableTaskResult.class));
        PageableTaskResult pageableTaskResult = (PageableTaskResult)taskResult;
        closeMeWhenDone = pageableTaskResult;
        assertThat(TestingHelpers.printedPage(pageableTaskResult.page()),
                is("1| Arthur| false" + System.lineSeparator() +
                        "2| Ford| false" + System.lineSeparator())
        );

        PageableTaskResult samePageResult = pageableTaskResult.fetch(pageInfo).get();
        closeMeWhenDone = samePageResult;

        assertThat(TestingHelpers.printedPage(samePageResult.page()),
                is("1| Arthur| false" + System.lineSeparator() +
                        "2| Ford| false" + System.lineSeparator())
        );

        pageInfo = new PageInfo(pageInfo.position(), pageInfo.size()+2);
        PageableTaskResult biggerPageResult = pageableTaskResult.fetch(pageInfo).get();
        assertThat(TestingHelpers.printedPage(biggerPageResult.page()),
                is("1| Arthur| false" + System.lineSeparator() +
                   "2| Ford| false" + System.lineSeparator() +
                   "3| Trillian| true" + System.lineSeparator() +
                   "4| Arthur| true" + System.lineSeparator())
        );

    }

    @Test
    public void testQueryThenFetchPageBackwards() throws Exception {
        setup.setUpCharacters();
        sqlExecutor.exec("insert into characters (id, name, female) values (?, ?, ?)", new Object[][]{
                new Object[]{
                        5, "Matthias Wahl", false,
                },
                new Object[]{
                        6, "Philipp Bogensberger", false,

                },
                new Object[]{
                        7, "Sebastian Utz", false
                }
        });
        sqlExecutor.refresh("characters");

        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        QueryThenFetchNode qtfNode = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(idRef, nameRef, femaleRef),
                Arrays.<Symbol>asList(idRef),
                new boolean[]{false},
                new Boolean[]{null},
                null,
                null,
                WhereClause.MATCH_ALL,
                null
        );

        List<Task> tasks = executor.newTasks(qtfNode, UUID.randomUUID());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = PageInfo.firstPage(2);

        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));
        ListenableFuture<TaskResult> resultFuture = results.get(0);

        TaskResult taskResult = resultFuture.get();
        assertThat(taskResult, instanceOf(PageableTaskResult.class));
        PageableTaskResult pageableTaskResult = (PageableTaskResult)taskResult;
        closeMeWhenDone = pageableTaskResult;
        assertThat(TestingHelpers.printedPage(pageableTaskResult.page()),
                is("1| Arthur| false" + System.lineSeparator() +
                   "2| Ford| false" + System.lineSeparator())
        );

        pageInfo = pageInfo.nextPage(4);
        PageableTaskResult nextResult = pageableTaskResult.fetch(pageInfo).get();
        closeMeWhenDone = nextResult;
        assertThat(TestingHelpers.printedPage(nextResult.page()), is(
                "3| Trillian| true" + System.lineSeparator() +
                "4| Arthur| true" + System.lineSeparator() +
                "5| Matthias Wahl| false" + System.lineSeparator() +
                "6| Philipp Bogensberger| false" + System.lineSeparator()));


        pageInfo = PageInfo.firstPage(1);
        PageableTaskResult backwardResult = nextResult.fetch(pageInfo).get();
        closeMeWhenDone = backwardResult;
        assertThat(TestingHelpers.printedPage(backwardResult.page()), is("1| Arthur| false" + System.lineSeparator()));
    }
}
