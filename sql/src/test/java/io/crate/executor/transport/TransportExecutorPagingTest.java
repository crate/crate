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
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Constants;
import io.crate.analyze.WhereClause;
import io.crate.executor.QueryResult;
import io.crate.executor.Task;
import io.crate.executor.TaskResult;
import io.crate.executor.pageable.Page;
import io.crate.executor.pageable.PageInfo;
import io.crate.executor.pageable.PageableTaskResult;
import io.crate.executor.pageable.policy.PageCachePolicy;
import io.crate.executor.task.join.NestedLoopTask;
import io.crate.executor.transport.task.elasticsearch.QueryThenFetchTask;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.node.dql.QueryThenFetchNode;
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class TransportExecutorPagingTest extends BaseTransportExecutorTest {

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
        qtfTask.setKeepAlive(TimeValue.timeValueSeconds(10));

        qtfTask.start(pageInfo, PageCachePolicy.NO_CACHE);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));
        ListenableFuture<TaskResult> resultFuture = results.get(0);

        TaskResult result = resultFuture.get();
        assertThat(result, instanceOf(PageableTaskResult.class));
        PageableTaskResult pageableResult = (PageableTaskResult)result;
        closeMeWhenDone = pageableResult;
        assertThat(TestingHelpers.printedPage(pageableResult.page()), is(
                "1| Arthur| false\n" +
                "4| Arthur| true\n"
        ));
        pageInfo = pageInfo.nextPage(2);
        ListenableFuture<Page> nextPageResultFuture = ((PageableTaskResult)result).fetch(pageInfo);
        Page nextPage = nextPageResultFuture.get();

        assertThat(TestingHelpers.printedPage(nextPage), is(
                "2| Ford| false\n" +
                "3| Trillian| true\n"
        ));

        pageInfo = pageInfo.nextPage(2);
        nextPage = pageableResult.fetch(pageInfo).get();
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
        qtfTask.setKeepAlive(TimeValue.timeValueSeconds(10));
        qtfTask.start(pageInfo, PageCachePolicy.NO_CACHE);
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
        ListenableFuture<Page> nextPageFuture = ((PageableTaskResult)result).fetch(pageInfo);
        Page nextPage = nextPageFuture.get();
        assertThat(TestingHelpers.printedPage(nextPage), is(
                "3| Trillian| true\n"
        ));

        pageInfo = pageInfo.nextPage(2);
        Page lastPage = pageableResult.fetch(pageInfo).get();
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
        qtfTask.setKeepAlive(TimeValue.timeValueSeconds(10));
        qtfTask.start(pageInfo, PageCachePolicy.NO_CACHE);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));

        ListenableFuture<TaskResult> resultFuture = results.get(0);
        TaskResult result = resultFuture.get();
        assertThat(result, instanceOf(PageableTaskResult.class));
        closeMeWhenDone = (PageableTaskResult)result;
        PageableTaskResult firstResult = (PageableTaskResult)result;
        assertThat(firstResult.page().size(), is(2L));

        pageInfo = pageInfo.nextPage(2);
        ListenableFuture<Page> nextPageFuture = firstResult.fetch(pageInfo);
        Page nextPage = nextPageFuture.get();
        assertThat(nextPage.size(), is(1L));

        pageInfo = pageInfo.nextPage(2);
        Page furtherPage = firstResult.fetch(pageInfo).get();
        assertThat(furtherPage.size(), is(0L));
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
        qtfTask.setKeepAlive(TimeValue.timeValueSeconds(10));
        qtfTask.start(pageInfo, PageCachePolicy.NO_CACHE);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));

        // first page
        ListenableFuture<TaskResult> resultFuture = results.get(0);
        TaskResult result = resultFuture.get();
        assertThat(result, instanceOf(PageableTaskResult.class));
        PageableTaskResult pageableResult = (PageableTaskResult)result;
        closeMeWhenDone = pageableResult;
        assertThat(pageableResult.page().size(), is(1L));

        Page nextPage;
        for (int i = 0; i<2 ; i++) {
            pageInfo = pageInfo.nextPage();
            ListenableFuture<Page> nextPageFuture = pageableResult.fetch(pageInfo);
            nextPage = nextPageFuture.get();
            assertThat(nextPage.size(), is(1L));
        }
        // no further pages
        assertThat(pageableResult.fetch(pageInfo.nextPage()).get().size(), is(0L));
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
        qtfTask.setKeepAlive(TimeValue.timeValueSeconds(10));
        qtfTask.start(pageInfo, PageCachePolicy.NO_CACHE);
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

        Page nextPage = ((PageableTaskResult) result).page();
        while (nextPage.size() > 0) {
            pageInfo = pageInfo.nextPage();
            ListenableFuture<Page> nextPageFuture = pageableResult.fetch(pageInfo);
            nextPage = nextPageFuture.get();
            pages.add(TestingHelpers.printedPage(nextPage));
        }

        assertThat(Joiner.on("").join(pages), is(
                "2| NULL| 0\n" +
                "3| Ford| 1396388720242\n"));
        // no further pages
        assertThat(nextPage.size(), is(0L));
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
        qtfTask.setKeepAlive(TimeValue.timeValueSeconds(10));
        qtfTask.start(pageInfo, PageCachePolicy.NO_CACHE);
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
        ListenableFuture<Page> nextPageFuture = pageableResult.fetch(pageInfo);
        Page nextPage = nextPageFuture.get();
        assertThat(TestingHelpers.printedPage(nextPage), is(
                "4| Arthur| true\n"));

        pageInfo = pageInfo.nextPage(5);
        nextPageFuture = pageableResult.fetch(pageInfo);
        nextPage = nextPageFuture.get();
        assertThat(TestingHelpers.printedPage(nextPage), is(
                "5| Matthias Wahl| false\n" +
                "6| Philipp Bogensberger| false\n" +
                "7| Sebastian Utz| false\n"));

        // no further pages
        assertThat(pageableResult.fetch(pageInfo.nextPage()).get().size(), is(0L));
    }

    @Test
    public void testNestedLoopBothSidesPageableNoLimit() throws Exception {
        setup.setUpCharacters();
        setup.setUpBooks();

        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        QueryThenFetchNode leftNode = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(idRef, nameRef, femaleRef),
                Arrays.<Symbol>asList(nameRef, femaleRef),
                new boolean[]{false, true},
                new Boolean[]{null, null},
                5,
                0,
                WhereClause.MATCH_ALL,
                null
        );
        leftNode.outputTypes(ImmutableList.of(
                        idRef.info().type(),
                        nameRef.info().type(),
                        femaleRef.info().type())
        );

        DocTableInfo books = docSchemaInfo.getTableInfo("books");
        QueryThenFetchNode rightNode = new QueryThenFetchNode(
                books.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(titleRef),
                Arrays.<Symbol>asList(titleRef),
                new boolean[]{false},
                new Boolean[]{null},
                null,
                null,
                WhereClause.MATCH_ALL,
                null
        );
        rightNode.outputTypes(ImmutableList.of(
                        authorRef.info().type())
        );

        TopNProjection projection = new TopNProjection(Constants.DEFAULT_SELECT_LIMIT, 0);
        projection.outputs(ImmutableList.<Symbol>of(
                new InputColumn(0, DataTypes.INTEGER),
                new InputColumn(1, DataTypes.STRING),
                new InputColumn(2, DataTypes.BOOLEAN),
                new InputColumn(3, DataTypes.STRING)
        ));
        List<DataType> outputTypes = ImmutableList.of(
                idRef.info().type(),
                nameRef.info().type(),
                femaleRef.info().type(),
                titleRef.info().type());


        // SELECT characters.id, characters.name, characters.female, books.title
        // FROM characters CROSS JOIN books
        // ORDER BY character.name, character.female, books.title
        NestedLoopNode node = new NestedLoopNode(leftNode, rightNode, true, Constants.DEFAULT_SELECT_LIMIT, 0);
        node.projections(ImmutableList.<Projection>of(projection));
        node.outputTypes(outputTypes);

        List<Task> tasks = executor.newTasks(node, UUID.randomUUID());
        assertThat(tasks.size(), is(1));
        assertThat(tasks.get(0), instanceOf(NestedLoopTask.class));

        NestedLoopTask nestedLoopTask = (NestedLoopTask) tasks.get(0);

        List<ListenableFuture<TaskResult>> results = nestedLoopTask.result();
        assertThat(results.size(), is(1));
        nestedLoopTask.start();
        TaskResult result = results.get(0).get();
        assertThat(result, instanceOf(QueryResult.class));
        assertThat(TestingHelpers.printedTable(result.rows()), is(
                        "4| Arthur| true| Life, the Universe and Everything\n" +
                        "4| Arthur| true| The Hitchhiker's Guide to the Galaxy\n" +
                        "4| Arthur| true| The Restaurant at the End of the Universe\n" +
                        "1| Arthur| false| Life, the Universe and Everything\n" +
                        "1| Arthur| false| The Hitchhiker's Guide to the Galaxy\n" +
                        "1| Arthur| false| The Restaurant at the End of the Universe\n" +
                        "2| Ford| false| Life, the Universe and Everything\n" +
                        "2| Ford| false| The Hitchhiker's Guide to the Galaxy\n" +
                        "2| Ford| false| The Restaurant at the End of the Universe\n" +
                        "3| Trillian| true| Life, the Universe and Everything\n" +
                        "3| Trillian| true| The Hitchhiker's Guide to the Galaxy\n" +
                        "3| Trillian| true| The Restaurant at the End of the Universe\n"));
    }

    @Test
    public void testNestedLoopBothSidesPageableLimitAndOffset() throws Exception {
        setup.setUpCharacters();
        setup.setUpBooks();

        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        QueryThenFetchNode leftNode = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(idRef, nameRef, femaleRef),
                Arrays.<Symbol>asList(nameRef, femaleRef),
                new boolean[]{false, true},
                new Boolean[]{null, null},
                5,
                0,
                WhereClause.MATCH_ALL,
                null
        );
        leftNode.outputTypes(ImmutableList.of(
                        idRef.info().type(),
                        nameRef.info().type(),
                        femaleRef.info().type())
        );

        DocTableInfo books = docSchemaInfo.getTableInfo("books");
        QueryThenFetchNode rightNode = new QueryThenFetchNode(
                books.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(titleRef),
                Arrays.<Symbol>asList(titleRef),
                new boolean[]{false},
                new Boolean[]{null},
                null,
                null,
                WhereClause.MATCH_ALL,
                null
        );
        rightNode.outputTypes(ImmutableList.of(
                        authorRef.info().type())
        );

        TopNProjection projection = new TopNProjection(10, 1);
        projection.outputs(ImmutableList.<Symbol>of(
                new InputColumn(0, DataTypes.INTEGER),
                new InputColumn(1, DataTypes.STRING),
                new InputColumn(2, DataTypes.BOOLEAN),
                new InputColumn(3, DataTypes.STRING)
        ));
        List<DataType> outputTypes = ImmutableList.of(
                idRef.info().type(),
                nameRef.info().type(),
                femaleRef.info().type(),
                titleRef.info().type());


        // SELECT characters.id, characters.name, characters.female, books.title
        // FROM characters CROSS JOIN books
        // ORDER BY character.name, character.female, books.title
        // limit 10 offset 1
        NestedLoopNode node = new NestedLoopNode(leftNode, rightNode, true, 10, 1);
        node.projections(ImmutableList.<Projection>of(projection));
        node.outputTypes(outputTypes);

        List<Task> tasks = executor.newTasks(node, UUID.randomUUID());
        assertThat(tasks.size(), is(1));
        assertThat(tasks.get(0), instanceOf(NestedLoopTask.class));

        NestedLoopTask nestedLoopTask = (NestedLoopTask) tasks.get(0);

        List<ListenableFuture<TaskResult>> results = nestedLoopTask.result();
        assertThat(results.size(), is(1));
        nestedLoopTask.start();
        TaskResult result = results.get(0).get();
        assertThat(result, instanceOf(QueryResult.class));
        assertThat(TestingHelpers.printedTable(result.rows()), is(
                        "4| Arthur| true| The Hitchhiker's Guide to the Galaxy\n" +
                        "4| Arthur| true| The Restaurant at the End of the Universe\n" +
                        "1| Arthur| false| Life, the Universe and Everything\n" +
                        "1| Arthur| false| The Hitchhiker's Guide to the Galaxy\n" +
                        "1| Arthur| false| The Restaurant at the End of the Universe\n" +
                        "2| Ford| false| Life, the Universe and Everything\n" +
                        "2| Ford| false| The Hitchhiker's Guide to the Galaxy\n" +
                        "2| Ford| false| The Restaurant at the End of the Universe\n" +
                        "3| Trillian| true| Life, the Universe and Everything\n" +
                        "3| Trillian| true| The Hitchhiker's Guide to the Galaxy\n"));
    }

    @Test
    public void testPagedNestedLoopWithProjectionsBothSidesPageable() throws Exception {
        setup.setUpCharacters();
        setup.setUpBooks();

        int queryOffset = 1;
        int queryLimit = 10;

        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        QueryThenFetchNode leftNode = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(idRef, nameRef, femaleRef),
                Arrays.<Symbol>asList(nameRef, femaleRef),
                new boolean[]{false, true},
                new Boolean[]{null, null},
                5,
                1,
                WhereClause.MATCH_ALL,
                null
        );
        leftNode.outputTypes(ImmutableList.of(
                        idRef.info().type(),
                        nameRef.info().type(),
                        femaleRef.info().type())
        );

        DocTableInfo books = docSchemaInfo.getTableInfo("books");
        QueryThenFetchNode rightNode = new QueryThenFetchNode(
                books.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(titleRef),
                Arrays.<Symbol>asList(titleRef),
                new boolean[]{false},
                new Boolean[]{null},
                null,
                null,
                WhereClause.MATCH_ALL,
                null
        );
        rightNode.outputTypes(ImmutableList.of(
                        authorRef.info().type())
        );

        TopNProjection projection = new TopNProjection(queryLimit, queryOffset);
        projection.outputs(ImmutableList.<Symbol>of(
                new InputColumn(0, DataTypes.INTEGER),
                new InputColumn(1, DataTypes.STRING),
                new InputColumn(2, DataTypes.BOOLEAN),
                new InputColumn(3, DataTypes.STRING)
        ));
        List<DataType> outputTypes = ImmutableList.of(
                idRef.info().type(),
                nameRef.info().type(),
                femaleRef.info().type(),
                titleRef.info().type());

        // SELECT c.id, c.name, c.female, b.title
        // FROM characters AS c CROSS JOIN books as b
        // ORDER BY c.name, c.female, b.title
        // LIMIT 10 OFFSET 1
        NestedLoopNode node = new NestedLoopNode(leftNode, rightNode, true, queryLimit, queryOffset);
        node.projections(ImmutableList.<Projection>of(projection));
        node.outputTypes(outputTypes);

        List<Task> tasks = executor.newTasks(node, UUID.randomUUID());
        assertThat(tasks.size(), is(1));
        assertThat(tasks.get(0), instanceOf(NestedLoopTask.class));

        NestedLoopTask nestedLoopTask = (NestedLoopTask) tasks.get(0);

        List<ListenableFuture<TaskResult>> results = nestedLoopTask.result();
        assertThat(results.size(), is(1));

        ListenableFuture<TaskResult> nestedLoopResultFuture = results.get(0);
        PageInfo pageInfo = new PageInfo(0, 2);
        nestedLoopTask.start(pageInfo, PageCachePolicy.NO_CACHE);

        TaskResult result = nestedLoopResultFuture.get();
        assertThat(result, instanceOf(PageableTaskResult.class));

        PageableTaskResult pageableResult = (PageableTaskResult)result;
        closeMeWhenDone = pageableResult;

        Page page = pageableResult.page();
        assertThat(page.size(), is(2L));

        assertThat(TestingHelpers.printedPage(page), is(
                "1| Arthur| false| The Hitchhiker's Guide to the Galaxy\n" +
                "1| Arthur| false| The Restaurant at the End of the Universe\n"));

        pageInfo = pageInfo.nextPage(1);
        Page secondPage = pageableResult.fetch(pageInfo).get();
        assertThat(secondPage.size(), is(1L));

        assertThat(TestingHelpers.printedPage(secondPage), is(
                "2| Ford| false| Life, the Universe and Everything\n"));


        pageInfo = pageInfo.nextPage(10);
        Page lastPage = pageableResult.fetch(pageInfo).get();
        assertThat(lastPage.size(), is(5L));

        assertThat(TestingHelpers.printedPage(lastPage), is(
                "2| Ford| false| The Hitchhiker's Guide to the Galaxy\n" +
                "2| Ford| false| The Restaurant at the End of the Universe\n" +
                "3| Trillian| true| Life, the Universe and Everything\n" +
                "3| Trillian| true| The Hitchhiker's Guide to the Galaxy\n" +
                "3| Trillian| true| The Restaurant at the End of the Universe\n"
        ));

        expectedException.expect(ExecutionException.class);
        expectedException.expectCause(TestingHelpers.cause(NoSuchElementException.class, "backingArray exceeded"));
        pageableResult.fetch(pageInfo.nextPage()).get();
    }
}
