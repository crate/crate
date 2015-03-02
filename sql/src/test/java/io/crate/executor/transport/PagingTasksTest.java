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
import io.crate.Constants;
import io.crate.action.sql.SQLResponse;
import io.crate.analyze.WhereClause;
import io.crate.executor.*;
import io.crate.executor.task.join.NestedLoopTask;
import io.crate.executor.transport.task.elasticsearch.QueryThenFetchTask;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.projectors.TopN;
import io.crate.planner.IterablePlan;
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
import org.elasticsearch.common.util.ObjectArray;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
                null,
                false
        );

        Job fakeJob = new Job();
        List<Task> tasks = executor.newTasks(qtfNode, fakeJob);
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = PageInfo.firstPage(2);

        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));
        ListenableFuture<TaskResult> resultFuture = results.get(0);

        TaskResult result = resultFuture.get();
        assertThat(result, instanceOf(TaskResult.class));
        assertThat(TestingHelpers.printedPage(result.page()), is(
                "1| Arthur| false\n" +
                "4| Arthur| true\n"
        ));
        pageInfo = pageInfo.nextPage(2);
        ListenableFuture<TaskResult> nextPageResultFuture = result.fetch(pageInfo);
        TaskResult nextPageResult = nextPageResultFuture.get();
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
                null,
                false
        );

        List<Task> tasks = executor.newTasks(qtfNode, new Job());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = new PageInfo(1, 2);
        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));
        ListenableFuture<TaskResult> resultFuture = results.get(0);
        TaskResult result = resultFuture.get();
        assertThat(result, instanceOf(TaskResult.class));

        assertThat(TestingHelpers.printedPage(result.page()), is(
                "4| Arthur| true\n" +
                "2| Ford| false\n"
        ));
        pageInfo = pageInfo.nextPage(2);
        ListenableFuture<TaskResult> nextPageResultFuture = result.fetch(pageInfo);
        TaskResult nextPageResult = nextPageResultFuture.get();
        closeMeWhenDone = nextPageResult;
        assertThat(TestingHelpers.printedPage(nextPageResult.page()), is(
                "3| Trillian| true\n"
        ));

        assertThat(nextPageResult.page().isLastPage(), is(true));
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
                null,
                false
        );

        List<Task> tasks = executor.newTasks(qtfNode, new Job());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = new PageInfo(1, 2);
        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));

        ListenableFuture<TaskResult> resultFuture = results.get(0);
        TaskResult result = resultFuture.get();
        assertThat(result, instanceOf(TaskResult.class));
        closeMeWhenDone = result;
        assertThat(result.page().size(), is(2L));

        pageInfo = pageInfo.nextPage(2);
        ListenableFuture<TaskResult> nextPageResultFuture = result.fetch(pageInfo);
        TaskResult nextPageResult = nextPageResultFuture.get();
        closeMeWhenDone = nextPageResult;
        assertThat(nextPageResult.page().size(), is(1L));

        pageInfo = pageInfo.nextPage(2);
        TaskResult furtherPageResult = nextPageResult.fetch(pageInfo).get();
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
                null,
                false
        );

        List<Task> tasks = executor.newTasks(qtfNode, new Job());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = new PageInfo(1, 1);
        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));

        // first page
        ListenableFuture<TaskResult> resultFuture = results.get(0);
        TaskResult result = resultFuture.get();
        assertThat(result, instanceOf(TaskResult.class));
        TaskResult pageableResult = (TaskResult)result;
        closeMeWhenDone = pageableResult;
        assertThat(pageableResult.page().size(), is(1L));

        TaskResult nextPageResult = (TaskResult)result;
        for (int i = 0; i<2 ; i++) {
            pageInfo = pageInfo.nextPage();
            ListenableFuture<TaskResult> nextPageResultFuture = nextPageResult.fetch(pageInfo);
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
                parted.partitionedByColumns(),
                false
        );

        List<Task> tasks = executor.newTasks(qtfNode, new Job());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = new PageInfo(0, 1);
        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));

        // first page
        ListenableFuture<TaskResult> resultFuture = results.get(0);
        TaskResult result = resultFuture.get();
        assertThat(result, instanceOf(TaskResult.class));
        TaskResult pageableResult = (TaskResult)result;
        closeMeWhenDone = pageableResult;
        assertThat(TestingHelpers.printedPage(pageableResult.page()), is("1| Trillian| NULL\n"));

        List<String> pages = new ArrayList<>();

        TaskResult nextPageResult = (TaskResult)result;
        while (nextPageResult.page().size() > 0) {
            pageInfo = pageInfo.nextPage();
            ListenableFuture<TaskResult> nextPageResultFuture = nextPageResult.fetch(pageInfo);
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
                null,
                false
        );

        List<Task> tasks = executor.newTasks(qtfNode, new Job());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = new PageInfo(1, 1);
        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));

        // first page
        ListenableFuture<TaskResult> resultFuture = results.get(0);
        TaskResult result = resultFuture.get();
        assertThat(result, instanceOf(TaskResult.class));
        closeMeWhenDone = result;

        // the two first records were hit by the query and page offset
        assertThat(TestingHelpers.printedPage(result.page()), is(
                "3| Trillian| true\n"));

        pageInfo = pageInfo.nextPage(1);
        ListenableFuture<TaskResult> nextPageResultFuture = result.fetch(pageInfo);
        TaskResult nextPageResult = nextPageResultFuture.get();
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
                null,
                false
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
                null,
                false
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
        NestedLoopNode node = new NestedLoopNode(
                new IterablePlan(leftNode),
                new IterablePlan(rightNode),
                true,
                Constants.DEFAULT_SELECT_LIMIT,
                0
        );
        node.projections(ImmutableList.<Projection>of(projection));
        node.outputTypes(outputTypes);

        List<Task> tasks = executor.newTasks(node, new Job());
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
                null,
                false
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
                null,
                false
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
        NestedLoopNode node = new NestedLoopNode(
                new IterablePlan(leftNode),
                new IterablePlan(rightNode),
                true,
                10,
                1
        );
        node.projections(ImmutableList.<Projection>of(projection));
        node.outputTypes(outputTypes);

        List<Task> tasks = executor.newTasks(node, new Job());
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
                null,
                false
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
                null,
                false
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
        NestedLoopNode node = new NestedLoopNode(
                new IterablePlan(leftNode),
                new IterablePlan(rightNode),
                true,
                queryLimit,
                queryOffset
        );
        node.projections(ImmutableList.<Projection>of(projection));
        node.outputTypes(outputTypes);

        List<Task> tasks = executor.newTasks(node, new Job());
        assertThat(tasks.size(), is(1));
        assertThat(tasks.get(0), instanceOf(NestedLoopTask.class));

        NestedLoopTask nestedLoopTask = (NestedLoopTask) tasks.get(0);

        List<ListenableFuture<TaskResult>> results = nestedLoopTask.result();
        assertThat(results.size(), is(1));

        ListenableFuture<TaskResult> nestedLoopResultFuture = results.get(0);
        PageInfo pageInfo = new PageInfo(0, 2);
        nestedLoopTask.start(pageInfo);

        TaskResult result = nestedLoopResultFuture.get();
        assertThat(result, instanceOf(TaskResult.class));

        TaskResult pageableResult = (TaskResult)result;
        closeMeWhenDone = pageableResult;

        Page firstPage = pageableResult.page();
        assertThat(firstPage.size(), is(2L));

        assertThat(TestingHelpers.printedPage(firstPage), is(
                "1| Arthur| false| The Hitchhiker's Guide to the Galaxy\n" +
                "1| Arthur| false| The Restaurant at the End of the Universe\n"));

        pageInfo = pageInfo.nextPage(1);
        pageableResult = pageableResult.fetch(pageInfo).get();
        closeMeWhenDone = pageableResult;

        Page secondPage = pageableResult.page();
        assertThat(secondPage.size(), is(1L));

        assertThat(TestingHelpers.printedPage(secondPage), is(
                "2| Ford| false| Life, the Universe and Everything\n"));


        pageInfo = pageInfo.nextPage(10);
        pageableResult = pageableResult.fetch(pageInfo).get();
        closeMeWhenDone = pageableResult;

        Page lastPage = pageableResult.page();
        assertThat(lastPage.size(), is(5L));

        assertThat(TestingHelpers.printedPage(lastPage), is(
                "2| Ford| false| The Hitchhiker's Guide to the Galaxy\n" +
                "2| Ford| false| The Restaurant at the End of the Universe\n" +
                "3| Trillian| true| Life, the Universe and Everything\n" +
                "3| Trillian| true| The Hitchhiker's Guide to the Galaxy\n" +
                "3| Trillian| true| The Restaurant at the End of the Universe\n"
        ));

        assertThat(pageableResult.fetch(pageInfo.nextPage()).get().page().size(), is(0L));
    }

    @Test
    public void testPagedNestedLoopOptimizedPaging() throws Exception {
        // optimized paging conditions:
        //  * no projections

        setup.setUpCharacters();
        setup.setUpBooks();

        int queryOffset = 0;
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
                null,
                false
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
                null,
                false
        );
        rightNode.outputTypes(ImmutableList.of(
                        authorRef.info().type())
        );

        List<DataType> outputTypes = ImmutableList.of(
                idRef.info().type(),
                nameRef.info().type(),
                femaleRef.info().type(),
                titleRef.info().type());

        // SELECT c.id, c.name, c.female, b.title
        // FROM characters AS c CROSS JOIN books as b
        // ORDER BY c.name, c.female, b.title
        // LIMIT 10 OFFSET 1
        NestedLoopNode node = new NestedLoopNode(
                new IterablePlan(leftNode),
                new IterablePlan(rightNode),
                true,
                queryLimit,
                queryOffset
        );
        node.outputTypes(outputTypes);

        List<Task> tasks = executor.newTasks(node, new Job());
        assertThat(tasks.size(), is(1));
        assertThat(tasks.get(0), instanceOf(NestedLoopTask.class));

        NestedLoopTask nestedLoopTask = (NestedLoopTask) tasks.get(0);

        List<ListenableFuture<TaskResult>> results = nestedLoopTask.result();
        assertThat(results.size(), is(1));

        ListenableFuture<TaskResult> nestedLoopResultFuture = results.get(0);
        PageInfo pageInfo = new PageInfo(1, 2);
        nestedLoopTask.start(pageInfo);

        TaskResult result = nestedLoopResultFuture.get();
        assertThat(result, instanceOf(TaskResult.class));

        TaskResult pageableResult = (TaskResult)result;
        closeMeWhenDone = pageableResult;

        Page firstPage = pageableResult.page();
        assertThat(firstPage.size(), is(2L));

        Field pageSourceField = BigArrayPage.class.
                getDeclaredField("page");
        pageSourceField.setAccessible(true);
        ObjectArray<Object[]> pageSource = (ObjectArray<Object[]>) pageSourceField.get(firstPage);
        assertThat(pageSource.size(), is((long)(pageInfo.position() + pageInfo.size())));

        assertThat(TestingHelpers.printedPage(firstPage), is(
                "1| Arthur| false| The Hitchhiker's Guide to the Galaxy\n" +
                "1| Arthur| false| The Restaurant at the End of the Universe\n"));

        pageInfo = pageInfo.nextPage(1);
        pageableResult = pageableResult.fetch(pageInfo).get();
        closeMeWhenDone = pageableResult;

        Page secondPage = pageableResult.page();
        assertThat(secondPage.size(), is(1L));
        pageSource = (ObjectArray<Object[]>) pageSourceField.get(secondPage);
        assertThat(pageSource.size(), is((long)(pageInfo.size())));

        assertThat(TestingHelpers.printedPage(secondPage), is(
                "2| Ford| false| Life, the Universe and Everything\n"));


        pageInfo = pageInfo.nextPage(10);
        pageableResult = pageableResult.fetch(pageInfo).get();
        closeMeWhenDone = pageableResult;

        Page lastPage = pageableResult.page();
        assertThat(lastPage.size(), is(5L));
        pageSource = (ObjectArray<Object[]>) pageSourceField.get(lastPage);
        assertThat(pageSource.size(), is(5L)); // only 5 left

        assertThat(TestingHelpers.printedPage(lastPage), is(
                "2| Ford| false| The Hitchhiker's Guide to the Galaxy\n" +
                "2| Ford| false| The Restaurant at the End of the Universe\n" +
                "3| Trillian| true| Life, the Universe and Everything\n" +
                "3| Trillian| true| The Hitchhiker's Guide to the Galaxy\n" +
                "3| Trillian| true| The Restaurant at the End of the Universe\n"
        ));

        assertThat(pageableResult.fetch(pageInfo.nextPage()).get().page().size(), is(0L));
    }

    @Test
    public void testPagedNestedLoopNestedCrossJoin() throws Exception {
        setup.setUpCharacters();
        setup.setUpBooks();

        int queryOffset = 1;
        int queryLimit = 20;

        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        QueryThenFetchNode leftQtfNode = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(idRef, nameRef, femaleRef),
                Arrays.<Symbol>asList(nameRef, femaleRef),
                new boolean[]{false, true},
                new Boolean[]{null, null},
                20,
                0,
                WhereClause.MATCH_ALL,
                null,
                false
        );
        leftQtfNode.outputTypes(ImmutableList.of(
                        idRef.info().type(),
                        nameRef.info().type(),
                        femaleRef.info().type())
        );

        DocTableInfo books = docSchemaInfo.getTableInfo("books");
        QueryThenFetchNode rightQtfNode = new QueryThenFetchNode(
                books.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(titleRef),
                Arrays.<Symbol>asList(titleRef),
                new boolean[]{false},
                new Boolean[]{null},
                null,
                null,
                WhereClause.MATCH_ALL,
                null,
                false
        );
        rightQtfNode.outputTypes(ImmutableList.of(
                        authorRef.info().type())
        );

        // self join :)
        NestedLoopNode leftNestedLoopNode = new NestedLoopNode(
                new IterablePlan(leftQtfNode),
                new IterablePlan(leftQtfNode),
                true,
                TopN.NO_LIMIT,
                TopN.NO_OFFSET
        );
        leftNestedLoopNode.outputTypes(
                ImmutableList.<DataType>builder()
                        .addAll(leftQtfNode.outputTypes())
                        .addAll(leftQtfNode.outputTypes())
                        .build()
        );

        // SELECT c1.id, c1.name, c1.female, c2.id, c2.name, c2.female, b.title
        // FROM characters c1 CROSS JOIN characters c2 CROSS JOIN books b
        // ORDER BY c1.name, c1.female, c2.name, c2.female, b.title
        // LIMIT 20 OFFSET 1;
        NestedLoopNode node = new NestedLoopNode(
                new IterablePlan(leftNestedLoopNode),
                new IterablePlan(rightQtfNode),
                true,
                queryLimit,
                queryOffset
        );
        TopNProjection projection = new TopNProjection(queryLimit, queryOffset);
        projection.outputs(ImmutableList.<Symbol>of(
                new InputColumn(0, DataTypes.INTEGER),
                new InputColumn(1, DataTypes.STRING),
                new InputColumn(2, DataTypes.BOOLEAN),
                new InputColumn(3, DataTypes.INTEGER),
                new InputColumn(4, DataTypes.STRING),
                new InputColumn(5, DataTypes.BOOLEAN),
                new InputColumn(6, DataTypes.STRING)
        ));
        List<DataType> outputTypes = ImmutableList.<DataType>builder()
                .addAll(leftQtfNode.outputTypes())
                .addAll(leftQtfNode.outputTypes())
                .addAll(rightQtfNode.outputTypes())
                .build();
        node.projections(ImmutableList.<Projection>of(projection));
        node.outputTypes(outputTypes);

        List<Task> tasks = executor.newTasks(node, new Job());
        assertThat(tasks.size(), is(1));
        assertThat(tasks.get(0), instanceOf(NestedLoopTask.class));


        NestedLoopTask nestedLoopTask = (NestedLoopTask) tasks.get(0);

        List<ListenableFuture<TaskResult>> results = nestedLoopTask.result();
        assertThat(results.size(), is(1));

        ListenableFuture<TaskResult> nestedLoopResultFuture = results.get(0);
        PageInfo pageInfo = new PageInfo(1, 5);
        nestedLoopTask.start(pageInfo);

        TaskResult result = nestedLoopResultFuture.get();
        assertThat(result, instanceOf(TaskResult.class));

        TaskResult pageableResult = result;
        closeMeWhenDone = pageableResult;

        Page firstPage = pageableResult.page();
        assertThat(firstPage.size(), is(5L));

        assertThat(TestingHelpers.printedPage(firstPage), is(
                "4| Arthur| true| 4| Arthur| true| The Restaurant at the End of the Universe\n" +
                "4| Arthur| true| 1| Arthur| false| Life, the Universe and Everything\n" +
                "4| Arthur| true| 1| Arthur| false| The Hitchhiker's Guide to the Galaxy\n" +
                "4| Arthur| true| 1| Arthur| false| The Restaurant at the End of the Universe\n" +
                "4| Arthur| true| 2| Ford| false| Life, the Universe and Everything\n"));

        pageInfo = pageInfo.nextPage(100);
        pageableResult = pageableResult.fetch(pageInfo).get();
        closeMeWhenDone = pageableResult;

        Page secondPage = pageableResult.page();
        assertThat(secondPage.size(), is(14L));

        assertThat(TestingHelpers.printedPage(secondPage), is(
                "4| Arthur| true| 2| Ford| false| The Hitchhiker's Guide to the Galaxy\n" +
                "4| Arthur| true| 2| Ford| false| The Restaurant at the End of the Universe\n" +
                "4| Arthur| true| 3| Trillian| true| Life, the Universe and Everything\n" +
                "4| Arthur| true| 3| Trillian| true| The Hitchhiker's Guide to the Galaxy\n" +
                "4| Arthur| true| 3| Trillian| true| The Restaurant at the End of the Universe\n" +
                "1| Arthur| false| 4| Arthur| true| Life, the Universe and Everything\n" +
                "1| Arthur| false| 4| Arthur| true| The Hitchhiker's Guide to the Galaxy\n" +
                "1| Arthur| false| 4| Arthur| true| The Restaurant at the End of the Universe\n" +
                "1| Arthur| false| 1| Arthur| false| Life, the Universe and Everything\n" +
                "1| Arthur| false| 1| Arthur| false| The Hitchhiker's Guide to the Galaxy\n" +
                "1| Arthur| false| 1| Arthur| false| The Restaurant at the End of the Universe\n" +
                "1| Arthur| false| 2| Ford| false| Life, the Universe and Everything\n" +
                "1| Arthur| false| 2| Ford| false| The Hitchhiker's Guide to the Galaxy\n" +
                "1| Arthur| false| 2| Ford| false| The Restaurant at the End of the Universe\n"
        ));
        assertThat(pageableResult.fetch(pageInfo.nextPage()).get().page().size(), is(0L));
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
                null,
                false
        );

        List<Task> tasks = executor.newTasks(qtfNode, new Job());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = PageInfo.firstPage(2);

        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));
        ListenableFuture<TaskResult> resultFuture = results.get(0);

        TaskResult taskResult = resultFuture.get();
        assertThat(taskResult, instanceOf(TaskResult.class));
        TaskResult pageableTaskResult = (TaskResult)taskResult;
        closeMeWhenDone = pageableTaskResult;
        assertThat(TestingHelpers.printedPage(pageableTaskResult.page()),
                is("1| Arthur| false\n" +
                   "2| Ford| false\n")
        );

        pageInfo = new PageInfo(4, 2);
        TaskResult gappedResult = pageableTaskResult.fetch(pageInfo).get();
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
                null,
                false
        );

        List<Task> tasks = executor.newTasks(qtfNode, new Job());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = PageInfo.firstPage(2);

        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));
        ListenableFuture<TaskResult> resultFuture = results.get(0);

        TaskResult taskResult = resultFuture.get();
        assertThat(taskResult, instanceOf(TaskResult.class));
        TaskResult pageableTaskResult = (TaskResult)taskResult;
        closeMeWhenDone = pageableTaskResult;
        assertThat(TestingHelpers.printedPage(pageableTaskResult.page()),
                is("0\n" +
                   "1\n")
        );

        pageInfo = new PageInfo(1048, 4);
        TaskResult gappedResult = pageableTaskResult.fetch(pageInfo).get();
        closeMeWhenDone = gappedResult;
        assertThat(TestingHelpers.printedPage(gappedResult.page()),
                is("1048\n" +
                   "1049\n" +
                   "1050\n" +
                   "1051\n")
        );

        pageInfo = pageInfo.nextPage(10);
        TaskResult afterGappedResult = gappedResult.fetch(pageInfo).get();
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

    @Test
    public void testRandomQTFPaging() throws Exception {
        SQLResponse response = sqlExecutor.exec("create table ids (id long primary key) with (number_of_replicas=0)");
        assertThat(response.rowCount(), is(1L));
        sqlExecutor.ensureGreen();

        int numRows = randomIntBetween(1, 4096);
        Object[][] bulkArgs = new Object[numRows][];
        for (int l = 0; l < numRows; l++) {
            bulkArgs[l] = new Object[]{l};
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
                null,
                false
        );

        List<Task> tasks = executor.newTasks(qtfNode, new Job());
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
        closeMeWhenDone = taskResult;

        int run = 0;
        while (run < 10) {
            Page page = taskResult.page();
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
            taskResult = taskResult.fetch(pageInfo).get();
            closeMeWhenDone = taskResult;
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
                null,
                false
        );

        List<Task> tasks = executor.newTasks(qtfNode, new Job());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = PageInfo.firstPage(2);

        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));
        ListenableFuture<TaskResult> resultFuture = results.get(0);

        TaskResult taskResult = resultFuture.get();
        assertThat(taskResult, instanceOf(TaskResult.class));
        closeMeWhenDone = taskResult;
        assertThat(TestingHelpers.printedPage(taskResult.page()),
                is("1| Arthur| false" + System.lineSeparator() +
                        "2| Ford| false" + System.lineSeparator())
        );

        TaskResult samePageResult = taskResult.fetch(pageInfo).get();
        closeMeWhenDone = samePageResult;

        assertThat(TestingHelpers.printedPage(samePageResult.page()),
                is("1| Arthur| false" + System.lineSeparator() +
                        "2| Ford| false" + System.lineSeparator())
        );

        pageInfo = new PageInfo(pageInfo.position(), pageInfo.size()+2);
        TaskResult biggerPageResult = taskResult.fetch(pageInfo).get();
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
                null,
                false
        );

        List<Task> tasks = executor.newTasks(qtfNode, new Job());
        assertThat(tasks.size(), is(1));
        QueryThenFetchTask qtfTask = (QueryThenFetchTask)tasks.get(0);
        PageInfo pageInfo = PageInfo.firstPage(2);

        qtfTask.start(pageInfo);
        List<ListenableFuture<TaskResult>> results = qtfTask.result();
        assertThat(results.size(), is(1));
        ListenableFuture<TaskResult> resultFuture = results.get(0);

        TaskResult taskResult = resultFuture.get();
        assertThat(taskResult, instanceOf(TaskResult.class));
        TaskResult pageableTaskResult = (TaskResult)taskResult;
        closeMeWhenDone = pageableTaskResult;
        assertThat(TestingHelpers.printedPage(pageableTaskResult.page()),
                is("1| Arthur| false" + System.lineSeparator() +
                   "2| Ford| false" + System.lineSeparator())
        );

        pageInfo = pageInfo.nextPage(4);
        TaskResult nextResult = pageableTaskResult.fetch(pageInfo).get();
        closeMeWhenDone = nextResult;
        assertThat(TestingHelpers.printedPage(nextResult.page()), is(
                "3| Trillian| true" + System.lineSeparator() +
                "4| Arthur| true" + System.lineSeparator() +
                "5| Matthias Wahl| false" + System.lineSeparator() +
                "6| Philipp Bogensberger| false" + System.lineSeparator()));


        pageInfo = PageInfo.firstPage(1);
        TaskResult backwardResult = nextResult.fetch(pageInfo).get();
        closeMeWhenDone = backwardResult;
        assertThat(TestingHelpers.printedPage(backwardResult.page()), is("1| Arthur| false" + System.lineSeparator()));
    }
}
