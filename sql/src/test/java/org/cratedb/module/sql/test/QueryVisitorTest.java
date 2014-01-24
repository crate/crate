/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package org.cratedb.module.sql.test;

import com.google.common.collect.ImmutableSet;
import org.cratedb.DataType;
import org.cratedb.action.collect.BytesRefColumnReference;
import org.cratedb.action.groupby.aggregate.avg.AvgAggFunction;
import org.cratedb.action.parser.ColumnReferenceDescription;
import org.cratedb.action.parser.QueryPlanner;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.TableExecutionContext;
import org.cratedb.index.ColumnDefinition;
import org.cratedb.service.GlobalExpressionService;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.OrderByAmbiguousException;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.ValueNode;
import org.cratedb.stubs.HitchhikerMocks;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryVisitorTest {

    private ParsedStatement stmt;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test(expected = SQLParseException.class)
    public void testUnsupportedStatement() throws StandardException, IOException {
        execStatement("explain select * from x");
    }

    private String getSource() {
        return stmt.xcontent.toUtf8();
    }

    private ParsedStatement execStatement(String stmt) throws StandardException {
        return execStatement(stmt, new Object[]{});
    }

    private ParsedStatement execStatement(String sql, Object[] args) throws StandardException {
        NodeExecutionContext nec = mock(NodeExecutionContext.class);
        TableExecutionContext tec = mock(TableExecutionContext.class);

        int colIdx = 0;
        ColumnDefinition colDef = new ColumnDefinition("locations", "whatever", DataType.STRING, "plain", colIdx++, false, false);
        // Disable query planner here to save mocking
        Settings settings = ImmutableSettings.builder().put(QueryPlanner.SETTINGS_OPTIMIZE_PK_QUERIES, false).build();
        QueryPlanner queryPlanner = new QueryPlanner(settings);
        when(nec.queryPlanner()).thenReturn(queryPlanner);
        when(nec.availableAggFunctions()).thenReturn(HitchhikerMocks.aggFunctionMap);
        GlobalExpressionService globalExpressionService = new GlobalExpressionService(HitchhikerMocks.globalExpressions);
        when(nec.globalExpressionService()).thenReturn(globalExpressionService);
        when(nec.tableContext(null, "locations")).thenReturn(tec);
        when(tec.allCols()).thenReturn(ImmutableSet.of("a", "b"));
        when(tec.getColumnDefinition(anyString())).thenReturn(colDef);

        when(tec.getColumnDefinition("col")).thenReturn(
            new ColumnDefinition("locations", "col", DataType.STRING, "plain", colIdx++, false, false)
        );
        when(tec.getColumnDefinition("kind")).thenReturn(
                new ColumnDefinition("locations", "kind", DataType.OBJECT, "plain", colIdx++, false, false)
        );
        when(tec.getColumnDefinition("kind.x")).thenReturn(
                new ColumnDefinition("locations", "kind.x", DataType.STRING, "plain", colIdx++, false,
                        false));
        when(tec.getColumnDefinition("name")).thenReturn(
                new ColumnDefinition("locations", "name", DataType.STRING, "plain", colIdx++, false, false)
        );
        when(tec.getColumnDefinition("nothing")).thenReturn(null);
        when(tec.getColumnDefinition("bool")).thenReturn(
                new ColumnDefinition("locations", "bool", DataType.BOOLEAN, "plain", colIdx++, false, false)
        );
        when(tec.getColumnDefinition("numeric_field")).thenReturn(
                new ColumnDefinition("locations", "numeric_field", DataType.DOUBLE, "plain", colIdx++, false, false)
        );
        when(tec.getColumnDefinition("object_field")).thenReturn(
                new ColumnDefinition("locations", "object_field", DataType.OBJECT, "plain", colIdx++, false, false)
        );
        when(tec.getColumnDefinition("age")).thenReturn(
                new ColumnDefinition("locations", "age", DataType.INTEGER, null, colIdx++, false, false)
        );

        when(tec.hasCol(anyString())).thenReturn(true);

        when(tec.getCollectorExpression(any(ValueNode.class))).thenCallRealMethod();

        SQLParseService parseService = new SQLParseService(nec);
        stmt = parseService.parse(sql, args);
        return stmt;
    }

    @Test(expected = SQLParseException.class)
    public void testStatementWithUnsupportedNode() throws StandardException, IOException {
        execStatement(
                "select * from locations inner join planets on planets.name = locations.name");
    }

    @Test(expected = SQLParseException.class)
    public void testUnsupportedExistsClause() throws StandardException, IOException {
        execStatement("select * from locations where exists (select 1 from locations)");
    }

    @Test
    public void testSelectWithNumericConstantValue() throws StandardException, IOException {
        expectedException.expect(SQLParseException.class);
        execStatement("select 1 from locations");
    }

    @Test(expected = SQLParseException.class)
    public void testSelectWithCharConstantValue() throws StandardException, IOException {
        execStatement("select 'name' from locations");
    }

    @Test
    public void testSelectAllFromTable() throws StandardException, IOException {
        execStatement("select * from locations");
        assertEquals(
            "{\"fields\":[\"a\",\"b\"],\"query\":{\"match_all\":{}},\"size\":" + SQLParseService.DEFAULT_SELECT_LIMIT + "}",
            getSource()
        );
    }

    @Test
    public void testSelectDistinct() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage(
            "the DISTINCT keyword is currently only supported with COUNT(). Consider rewriting the query using a group by clause.");
        execStatement("select distinct name from locations");
    }

    @Test
    public void testSelectWithLimitAsParameter() throws Exception {
        Integer limit = 5;
        execStatement("SELECT name from locations limit ?", new Object[]{limit});
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("name"))
                        .startObject("query")
                        .field("match_all", new HashMap<String, Object>())
                        .endObject()
                        .field("size", limit)
                        .endObject()
                        .string();
        assertEquals(expected, getSource());
    }

    @Test
    public void testSelectWithLimitAsOffset() throws Exception {
        Integer limit = 1;
        Integer offset = 5;
        execStatement("SELECT name from locations limit " + limit + " offset ?", new Object[]{offset});
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("name"))
                        .startObject("query")
                        .field("match_all", new HashMap<String, Object>())
                        .endObject()
                        .field("from", offset)
                        .field("size", limit)
                        .endObject()
                        .string();
        assertEquals(expected, getSource());
    }

    @Test
    public void testSelectGroupByOrderByAmbiguousColumn() throws Exception {
        expectedException.expect(OrderByAmbiguousException.class);
        execStatement("select name, kind as name from locations group by name, kind order by name");
    }

    @Test
    public void testSelectOrderByAmbiguousColumn() throws Exception {
        expectedException.expect(OrderByAmbiguousException.class);
        execStatement("select name, kind as name from locations order by name");
    }

    @Test
    public void testOrderByAlias() throws Exception {
        execStatement("select name as n from locations order by n");
        String expected =
            XContentFactory.jsonBuilder()
                .startObject()
                .field("fields", Arrays.asList("name"))
                .startObject("query")
                .field("match_all", new HashMap())
                .endObject()
                .startArray("sort")
                .startObject()
                .startObject("name")
                .field("order", "asc")
                .field("ignore_unmapped", true)
                .endObject()
                .endObject()
                .endArray()
                .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                .endObject()
                .string();
        stmt.outputFields();

        assertEquals(expected, getSource());
    }

    @Test
    public void testSelectWithFieldAs() throws StandardException, IOException {

        execStatement("select name as n from locations");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("name"))
                        .startObject("query")
                        .field("match_all", new HashMap())
                        .endObject()
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();
        stmt.outputFields();

        assertEquals(expected, getSource());
        assertEquals("n", stmt.outputFields().get(0).v1());
        assertEquals("name", stmt.outputFields().get(0).v2());
    }

    @Test
    public void testSelectVersion() throws StandardException, IOException {

        execStatement("select \"_version\" from locations");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .field("match_all", new HashMap())
                        .endObject()
                        .field("version", true)
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
    }

    @Test
    public void testSelectAllAndFieldFromTable() throws StandardException, IOException {
        execStatement("select *, name from locations");
        assertEquals("{\"fields\":[\"a\",\"b\",\"name\"],\"query\":{\"match_all\":{}},\"size\":10000}",
                getSource());
    }

    @Test
    public void testSelectWithLimit() throws StandardException, IOException {
        execStatement("select * from locations limit 5");
        assertEquals("{\"fields\":[\"a\",\"b\"],\"query\":{\"match_all\":{}},\"size\":5}",
                getSource());
    }

    @Test
    public void testSelectWithHugeLimit() throws StandardException, IOException {
        execStatement("select * from locations limit 2000");
        assertEquals(
            "{\"fields\":[\"a\",\"b\"],\"query\":{\"match_all\":{}},\"size\":2000}",
            getSource()
        );

    }

    @Test
    public void testSelectWithLimitAndOffset() throws StandardException, IOException {
        execStatement("select * from locations limit 5 offset 3");
        assertEquals(
                "{\"fields\":[\"a\",\"b\"],\"query\":{\"match_all\":{}},\"from\":3,\"size\":5}",
                getSource());
    }

    @Test
    public void testSelectWithOrderBy() throws StandardException, IOException {

        execStatement("select * from locations order by kind");
        assertEquals(
                "{\"fields\":[\"a\",\"b\"]," +
                        "\"query\":{\"match_all\":{}}," +
                        "\"sort\":[{\"kind\":{\"order\":\"asc\",\"ignore_unmapped\":true}}]," +
                        "\"size\":" + SQLParseService.DEFAULT_SELECT_LIMIT +
                        "}", getSource());
    }

    @Test
    public void testSelectWithMultipleOrderBy() throws StandardException, IOException {

        execStatement("select * from locations order by kind asc, name desc");
        assertEquals(
                "{\"fields\":[\"a\",\"b\"]," +
                        "\"query\":{\"match_all\":{}}," +
                        "\"sort\":[{\"kind\":{\"order\":\"asc\",\"ignore_unmapped\":true}}," +
                        "{\"name\":{\"order\":\"desc\"," +
                        "\"ignore_unmapped\":true}}]," +
                        "" +
                        "\"size\":" + SQLParseService.DEFAULT_SELECT_LIMIT +
                        "}",
                getSource());
    }

    @Test
    public void testSelectFieldsFromTable() throws StandardException, IOException {

        execStatement("select name, kind from locations");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .startObject("query")
                        .field("match_all", new HashMap())
                        .endObject()
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
    }

    @Test
    public void testSelectNestedColumnsFromTable() throws StandardException, IOException {

        NodeExecutionContext nec = mock(NodeExecutionContext.class);
        TableExecutionContext tec = mock(TableExecutionContext.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        when(nec.queryPlanner()).thenReturn(queryPlanner);
        GlobalExpressionService globalExpressionService = new GlobalExpressionService(HitchhikerMocks.globalExpressions);
        when(nec.globalExpressionService()).thenReturn(globalExpressionService);
        when(nec.tableContext(null, "persons")).thenReturn(tec);
        when(tec.allCols()).thenReturn(ImmutableSet.of("message", "person"));

        String sql = "select persons.message, persons.person['addresses'] from persons " +
                "where person['name'] = 'Ford'";

        SQLParseService parseService = new SQLParseService(nec);
        stmt = parseService.parse(sql, new Object[0]);

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("message", "person.addresses"))
                        .startObject("query")
                        .startObject("term").field("person.name", "Ford").endObject()
                        .endObject()
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
        assertEquals(new Tuple<String, String>("message", "message"),
                stmt.outputFields().get(0));
        assertEquals(new Tuple<String, String>("person['addresses']",
                "person.addresses"),
                stmt.outputFields().get(1));
    }

    @Test(expected = SQLParseException.class)
    public void testUnsuportedNestedColumnIndexInWhereClause() throws StandardException,
            IOException {

        NodeExecutionContext nec = mock(NodeExecutionContext.class);
        TableExecutionContext tec = mock(TableExecutionContext.class);
        QueryPlanner queryPlanner = mock(QueryPlanner.class);
        when(nec.queryPlanner()).thenReturn(queryPlanner);
        when(nec.tableContext(null, "persons")).thenReturn(tec);
        String sql = "select persons.message, person['name'] from persons " +
                "where person['addresses'][0]['city'] = 'Berlin'";

        SQLParseService parseService = new SQLParseService(nec);
        stmt = parseService.parse(sql);
    }

    @Test(expected = SQLParseException.class)
    public void testUnsuportedNestedColumnIndexInFields() throws StandardException,
            IOException {

        NodeExecutionContext nec = mock(NodeExecutionContext.class);
        TableExecutionContext tec = mock(TableExecutionContext.class);
        when(nec.tableContext(null, "persons")).thenReturn(tec);
        String sql = "select persons.message, person['name'], person['addresses'][0] from persons";

        SQLParseService parseService = new SQLParseService(nec);
        stmt = parseService.parse(sql);
    }

    @Test
    public void testWhereClauseToTermsQuery() throws StandardException, IOException {
        execStatement("select name, kind from locations where name = 'Bartledan'");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .startObject("query")
                        .startObject("term").field("name", "Bartledan").endObject()
                        .endObject()
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();
        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseToTermsQueryWithUnderscoreField() throws StandardException,
            IOException {
        execStatement("select name, kind from locations where \"_id\" = 1");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .startObject("query")
                        .startObject("term").field("_id", 1).endObject()
                        .endObject()
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();
        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseWithNotEqual() throws StandardException, IOException {
        execStatement("select name, kind from locations where position != 1");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .startObject("query")
                        .startObject("bool")
                        .startObject("must_not")
                        .startObject("term").field("position", 1).endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();
        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseWithIsNull() throws StandardException, IOException {
        execStatement("select name from locations where name is null");

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("name"))
                        .startObject("query")
                        .startObject("filtered")
                        .startObject("filter")
                        .startObject("missing")
                        .field("field", "name")
                        .field("existence", true)
                        .field("null_value", true)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseWithIsNotNull() throws StandardException, IOException {
        execStatement("select name from locations where name is not null");

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("name"))
                        .startObject("query")
                        .startObject("bool")
                        .startObject("must_not")
                        .startObject("filtered")
                        .startObject("filter")
                        .startObject("missing")
                        .field("field", "name")
                        .field("existence", true)
                        .field("null_value", true)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseWithNotEqualLtGtSyntax() throws StandardException, IOException {
        execStatement("select name, kind from locations where position <> 1");

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .startObject("query")
                        .startObject("bool")
                        .startObject("must_not")
                        .startObject("term").field("position", 1).endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();
        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseToTermsQueryWithDateField() throws StandardException, IOException {
        execStatement("select name, kind from locations where date = '2013-07-16'");

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .startObject("query")
                        .startObject("term").field("date", "2013-07-16").endObject()
                        .endObject()
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();
        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseToTermsQueryWithNumberField() throws StandardException,
            IOException {
        execStatement("select * from locations where position = 4");

        assertEquals("{\"fields\":[\"a\",\"b\"],\"query\":{\"term\":{\"position\":4}},\"size\":" + SQLParseService.DEFAULT_SELECT_LIMIT + "}",
                getSource());
    }

    @Test
    public void testWhereClauseWithYodaCondition() throws StandardException, IOException {
        execStatement("select name, kind from locations where 'Bartledan' = name");

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .startObject("query")
                        .startObject("term").field("name", "Bartledan").endObject()
                        .endObject()
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseWithOneAnd() throws StandardException, IOException {
        execStatement("select name, kind from locations where 'Bartledan' = name and kind = " +
                "'Planet'");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .startObject("query")
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("must")
                        .startObject().startObject("term").field("name",
                        "Bartledan").endObject().endObject()
                        .startObject().startObject("term").field("kind",
                        "Planet").endObject().endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseWithOneOr() throws StandardException, IOException {
        execStatement("select name, kind from locations where kind = 'Galaxy' or kind = 'Planet'");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .startObject("query")
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("should")
                        .startObject().startObject("term").field("kind",
                        "Galaxy").endObject().endObject()
                        .startObject().startObject("term").field("kind",
                        "Planet").endObject().endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();

        String actual = getSource();
        assertEquals(expected, actual);
    }

    @Test
    public void testWhereClauseWithManyOr() throws StandardException, IOException {
        execStatement(
                "select name, kind from locations where kind = 'Galaxy' or kind = 'Planet' " +
                        "or kind = 'x' or kind = 'y'");
        // this query could be optimized in either an terms query or a simplified bool query
        // without the nesting.
        // but the SQL-Syntax Tree's structure makes this format easier to generate and ES
        // optimized this probably anyway later on.

        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .startObject("query")
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("should")
                        .startObject()
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("should")
                        .startObject()
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("should")
                        .startObject().startObject("term").field("kind",
                        "Galaxy").endObject().endObject()
                        .startObject().startObject("term").field("kind",
                        "Planet").endObject().endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .startObject().startObject("term").field("kind",
                        "x").endObject().endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .startObject().startObject("term").field("kind",
                        "y").endObject().endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();

        String actual = getSource();
        assertEquals(expected, actual);
    }

    @Test
    public void testWhereClauseWithOrAndNestedAnd() throws StandardException, IOException {
        execStatement("select name, kind from locations where name = 'Bartledan' or (kind = " +
                "'Planet' and \"_id\" = '11')");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .startObject("query")
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("should")
                        .startObject()
                        .startObject("term").field("name", "Bartledan").endObject()
                        .endObject()
                        .startObject()
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("must")
                        .startObject().startObject("term").field("kind",
                        "Planet").endObject().endObject()
                        .startObject().startObject("term").field("_id",
                        "11").endObject().endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseWithAndAndGreaterThan() throws StandardException, IOException {
        execStatement("select name, kind from locations where kind = 'Planet' and \"_id\" > '4'");
        String expected =
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("fields", Arrays.asList("name", "kind"))
                        .startObject("query")
                        .startObject("bool")
                        .field("minimum_should_match", 1)
                        .startArray("must")
                        .startObject().startObject("term").field("kind",
                        "Planet").endObject().endObject()
                        .startObject()
                        .startObject("range")
                        .startObject("_id")
                        .field("gt", "4")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .field("size", SQLParseService.DEFAULT_SELECT_LIMIT)
                        .endObject()
                        .string();

        assertEquals(expected, getSource());
    }

    @Test
    public void testWhereClauseToRangeQueryWithGtNumberField() throws StandardException,
            IOException {
        execStatement("select * from locations where position > 4");
        assertEquals("{\"fields\":[\"a\",\"b\"],\"query\":{\"range\":{\"position\":{\"gt\":4}}},\"size\":" + SQLParseService.DEFAULT_SELECT_LIMIT + "}",
                getSource());
    }

    @Test
    public void testWhereClauseToRangeQueryWithGteNumberField() throws StandardException,
            IOException {
        execStatement("select * from locations where position >= 4");
        assertEquals("{\"fields\":[\"a\",\"b\"],\"query\":{\"range\":{\"position\":{\"gte\":4}}},\"size\":" + SQLParseService.DEFAULT_SELECT_LIMIT + "}",
                getSource());
    }

    @Test
    public void testWhereClauseToRangeQueryWithGtNumberFieldYoda() throws StandardException,
            IOException {
        execStatement("select * from locations where 4 < position");
        assertEquals("{\"fields\":[\"a\",\"b\"],\"query\":{\"range\":{\"position\":{\"gt\":4}}},\"size\":" + SQLParseService.DEFAULT_SELECT_LIMIT + "}",
                getSource());
    }

    @Test
    public void testWhereClauseToRangeQueryWithLteNumberField() throws StandardException,
            IOException {
        execStatement("select * from locations where position <= 4");
        assertEquals("{\"fields\":[\"a\",\"b\"],\"query\":{\"range\":{\"position\":{\"lte\":4}}},\"size\":" + SQLParseService.DEFAULT_SELECT_LIMIT + "}",
                getSource());
    }

    @Test
    public void testWhereClauseInList() throws StandardException {
        execStatement("select * from locations where position in (?,?,?)", new Object[]{3, 5, 6});
        assertEquals("{\"fields\":[\"a\",\"b\"],\"query\":{\"terms\":{\"position\":[3,5,6]}},\"size\":10000}", getSource());
    }

    @Test
    public void testUpdateWhereClauseInList() throws StandardException {
        execStatement("update locations set name='foobar' where position in (?,?,?)", new Object[]{1,2,3});
        assertEquals("{\"query\":{\"terms\":{\"position\":[1,2,3]}},\"facets\":{\"sql\":{\"sql\":{\"stmt\":\"update locations set name='foobar' where position in (?,?,?)\",\"args\":[1,2,3]}}}}", getSource());
    }

    @Test
    public void testDeleteQuery() throws Exception {
        execStatement("delete from locations where 4 < position");
        assertEquals("{\"range\":{\"position\":{\"gt\":4}}}",
                getSource());
    }

    @Test
    public void testCountQuery() throws Exception {
        execStatement("select count(*) from locations where 4 < position");
        assertEquals("{\"range\":{\"position\":{\"gt\":4}},\"size\":" + SQLParseService.DEFAULT_SELECT_LIMIT + "}",
            getSource());
    }


    @Test
    public void testUpdate() throws Exception {
        execStatement("update locations set a=? where a=2", new Object[]{1});
        assertEquals("{\"query\":{\"term\":{\"a\":2}}," +
                "\"facets\":{\"sql\":{\"sql\":{\"stmt\":\"update locations set a=? where a=2\",\"args\":[1]}}}}", getSource());
    }

    @Test
    public void testUpdateDocWithArgs() throws Exception {
        execStatement("update locations set a=? where a=2", new Object[]{1});
        Map<String, Object> expected = new HashMap<String, Object>(1);
        expected.put("a", 1);
        assertEquals(expected, stmt.updateDoc());
    }

    @Test
    public void testUpdateDocWith2Args() throws Exception {
        execStatement("update locations set a=?,b=? where a=2", new Object[]{1, 2});
        Map<String, Object> expected = new HashMap<String, Object>(1);
        expected.put("a", 1);
        expected.put("b", 2);
        assertEquals(expected, stmt.updateDoc());
    }


    @Test
    public void testUpdateDocWithConstant() throws Exception {
        execStatement("update locations set a=1 where a=2");
        Map<String, Object> expected = new HashMap<String, Object>(1);
        expected.put("a", 1);
        assertEquals(expected, stmt.updateDoc());

    }

    @Test
    public void testSelectWithGroupBy() throws Exception {
        // limit and offset shouldn't be set in the xcontent because the limit has to be applied
        // after the grouping is done
        execStatement("select count(*), kind from locations group by kind limit 4 offset 3");
        assertEquals("{\"query\":{\"match_all\":{}}}", getSource());
    }

    @Test
    public void testSelectWithWhereLikePrefixQuery() throws Exception {
        execStatement("select kind from locations where kind like 'P%'");
        assertEquals(
            "{\"fields\":[\"kind\"],\"query\":{\"wildcard\":{\"kind\":\"P*\"}},\"size\":10000}",
            getSource()
        );
    }

    @Test
    public void testSelectWithWhereLikePrefixQueryEscaped() throws Exception {
        execStatement("select kind from locations where kind like 'P\\%'");
        assertEquals(
            "{\"fields\":[\"kind\"],\"query\":{\"wildcard\":{\"kind\":\"P%\"}},\"size\":10000}",
            getSource()
        );
    }

    @Test
    public void testSelectWithWhereLikeWithEscapedStar() throws Exception {
        execStatement("select kind from locations where kind like 'P*'");
        assertEquals(
            "{\"fields\":[\"kind\"],\"query\":{\"wildcard\":{\"kind\":\"P\\\\*\"}},\"size\":10000}",
            getSource()
        );
    }

    @Test
    public void testSelectWithWhereLikeWithoutWildcards() throws Exception {
        execStatement("select kind from locations where kind like 'P'");
        assertEquals(
            "{\"fields\":[\"kind\"],\"query\":{\"wildcard\":{\"kind\":\"P\"}},\"size\":10000}",
            getSource()
        );
    }

    @Test
    public void testSelectWithWhereLikePrefixQueryUnderscore() throws Exception {
        execStatement("select kind from locations where kind like 'P_'");
        assertEquals(
            "{\"fields\":[\"kind\"],\"query\":{\"wildcard\":{\"kind\":\"P?\"}},\"size\":10000}",
            getSource()
        );
    }

    @Test
    public void testSelectWithWhereLikePrefixQueryUnderscoreEscaped() throws Exception {
        execStatement("select kind from locations where kind like 'P\\_'");
        assertEquals(
                "{\"fields\":[\"kind\"],\"query\":{\"wildcard\":{\"kind\":\"P_\"}},\"size\":10000}",
                getSource()
        );
    }

    @Test
    public void testSelectWithWhereLikePrefixQueryQuestionmark() throws Exception {
        execStatement("select kind from locations where kind like 'P?'");
        assertEquals(
                "{\"fields\":[\"kind\"],\"query\":{\"wildcard\":{\"kind\":\"P\\\\?\"}},\"size\":10000}",
                getSource()
        );
    }

    @Test
    public void testSelectWithWhereLikeReversed() throws Exception {
        execStatement("select kind from locations where 'P_' like kind");
        assertEquals(
                "{\"fields\":[\"kind\"],\"query\":{\"wildcard\":{\"kind\":\"P?\"}},\"size\":10000}",
                getSource()
        );
    }

    @Test
    public void testGroupByWithNestedColumn() throws Exception {
        ParsedStatement stmt = execStatement("select count(*), kind['x'] from locations group by kind['x']");

        boolean found = false;

        assertEquals("kind.x",
                ((BytesRefColumnReference)stmt.groupByExpressions().get(0)).columnName());

        assertTrue(stmt.resultColumnList().contains(new ColumnReferenceDescription("kind.x", DataType.STRING)));
    }

    @Test
    public void testSelectByVersionException() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage(
                "_version is only valid in the WHERE clause if paired with a single primary key column and crate.planner.optimize.pk_queries enabled");
        execStatement("select kind from locations where \"_version\" = 1");
    }

    @Test
    public void testDeleteByVersionWithoutPlannerException() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage(
                "_version is only valid in the WHERE clause if paired with a single primary key column and crate.planner.optimize.pk_queries enabled");
        execStatement("delete from locations where \"_id\" = 1 and \"_version\" = 1");
    }

    @Test
    public void testSelectWithWhereMatch() throws Exception {
        execStatement("select kind from locations where match(kind, 'Star')");
        assertEquals(
                "{\"fields\":[\"kind\"],\"query\":{\"match\":{\"kind\":\"Star\"}},\"size\":10000}",
                getSource()
        );
    }

    @Test
    public void testSelectWithWhereNotMatch() throws Exception {
        execStatement("select kind from locations where not match(kind, 'Star')");
        assertEquals(
                "{\"fields\":[\"kind\"],\"query\":{\"bool\":{\"must_not\":{\"match\":{\"kind\":\"Star\"}}}},\"size\":10000}",
                getSource()
        );
    }

    @Test
    public void testSelectWithWhereMatchAnd() throws Exception {
        execStatement("select kind from locations where match(kind, 'Star') and name = 'Algol'");
        assertEquals(
                "{\"fields\":[\"kind\"],\"query\":{\"bool\":{\"minimum_should_match\":1,\"must\":[{\"match\":{\"kind\":\"Star\"}},{\"term\":{\"name\":\"Algol\"}}]}},\"size\":10000}",
                getSource()
        );
    }

    @Test
    public void testSelectWithOrderByScore() throws Exception {
        execStatement("select kind from locations where match(kind, ?) " +
                "order by \"_score\" desc",
                new Object[]{"Star", "Star"});
        assertEquals(
                "{\"fields\":[\"kind\"],\"query\":{\"match\":{\"kind\":\"Star\"}},\"sort\":[{\"_score\":{\"order\":\"desc\",\"ignore_unmapped\":true}}],\"size\":10000}",
                getSource()
        );
    }

    @Test
    public void testSelectSysColumnScore() throws Exception {
        execStatement("select kind, \"_score\" from locations where match(kind, ?) " +
                "order by \"_score\" desc",
                new Object[]{"Star", "Star"});
        assertEquals(
                "{\"fields\":[\"kind\"],\"query\":{\"match\":{\"kind\":\"Star\"}},\"sort\":[{\"_score\":{\"order\":\"desc\",\"ignore_unmapped\":true}}],\"size\":10000}",
                getSource()
        );
    }

    @Test
    public void testWhereClauseWithScore() throws Exception {
        execStatement("select kind, \"_score\" from locations where match(kind, ?) " +
                "and \"_score\" > 0.05",
                new Object[]{"Star", "Star"});
        assertEquals(
                "{\"fields\":[\"kind\"],\"query\":{\"bool\":{\"minimum_should_match\":1,\"must\":[{\"match\":{\"kind\":\"Star\"}},{\"match_all\":{}}]}},\"min_score\":0.05,\"size\":10000}",
                getSource()
        );
    }

    @Test
    public void testWhereClauseWithScoreInvalidEqualsOperator() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Filtering by _score can only be done using a " +
                "greater-than or greater-equals operator");
        execStatement("select kind, \"_score\" from locations where match(kind, ?) " +
                "and \"_score\" = 0.05",
                new Object[]{"Star", "Star"});
    }

    @Test
    public void testWhereClauseWithScoreInvalidLessOperator() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Filtering by _score can only be done using a " +
                "greater-than or greater-equals operator");
        execStatement("select kind, \"_score\" from locations where match(kind, ?) " +
                "and \"_score\" < 0.05",
                new Object[]{"Star", "Star"});
    }

    @Test
    public void testWhereClauseWithScoreInvalidLessEqualsOperator() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Filtering by _score can only be done using a " +
                "greater-than or greater-equals operator");
        execStatement("select kind, \"_score\" from locations where match(kind, ?) " +
                "and \"_score\" <= 0.05",
                new Object[]{"Star", "Star"});
    }

    @Test
    public void testMinAggWithoutArgs() throws Exception {
        expectedException.expect(SQLParseException.class);
        execStatement("select min() from locations group by departement");
    }

    @Test
    public void testMinWithNonExistingColumn() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Unknown column 'nothing'");
        execStatement("select min(nothing) from locations group by departement");
    }

    @Test
    public void testCountAggWithoutArgs() throws Exception {

        expectedException.expect(SQLParseException.class);
        execStatement("select count() from locations group by departement");
    }

    @Test
    public void selectGroupByAggregateMinStar() throws Exception {
        expectedException.expect(SQLParseException.class);

        execStatement("select min(*) from locations group by gender");
    }

    @Test
    public void testMaxAggWithoutArgs() throws Exception {
        expectedException.expect(SQLParseException.class);
        execStatement("select max() from locations group by departement");
    }

    @Test
    public void testMaxWithNonExistingColumn() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Unknown column 'nothing'");
        execStatement("select max(nothing) from locations group by departement");
    }

    @Test
    public void selectGroupByAggregateMaxStar() throws Exception {
        expectedException.expect(SQLParseException.class);

        execStatement("select max(*) from locations group by gender");
    }

    @Test
    public void testMaxWithWrongColumnType() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Invalid column type 'boolean' for aggregate function MAX");

        execStatement("select max(bool) from locations group by gender");
    }

    @Test
    public void testMinWithWrongColumnType() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Invalid column type 'boolean' for aggregate function MIN");

        execStatement("select min(bool) from locations group by gender");
    }

    @Test
    public void testGroupByAggSumInvalidColumn() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Invalid column type 'boolean' for aggregate function SUM");

        execStatement("select sum(bool) from locations group by stuff");
    }

    @Test
    public void testGroupByAggAvgInvalidColumn() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Invalid column type 'boolean' for aggregate function AVG");

        execStatement("select avg(bool) from locations group by stuff");
    }

    @Test
    public void testAggOnGlobalSelect() throws Exception {
        execStatement("select avg(numeric_field) from locations");
        assertEquals(1, stmt.aggregateExpressions().size());
        assertEquals(AvgAggFunction.NAME, stmt.aggregateExpressions().get(0).functionName);
        assertEquals(DataType.DOUBLE, stmt.aggregateExpressions().get(0).expression.returnType());

        assertTrue(stmt.isGlobalAggregate());
    }

    @Test
    public void testAggOnGlobalFalseOnGroupBy() throws Exception {
        execStatement("select avg(numeric_field) from locations group by stuff");
        assertFalse(stmt.isGlobalAggregate());
    }

    @Test
    public void testAggOnGlobalManyAggs() throws Exception {
        execStatement("select avg(numeric_field), sum(numeric_field), max(some_field) from locations");
        assertEquals(3, stmt.aggregateExpressions().size());
        assertTrue(stmt.isGlobalAggregate());
    }

    @Test
    public void testAggOnGlobalWithNormalColumns1() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Only aggregate expressions allowed here");

        execStatement("select avg(numeric_field), col from locations");
    }

    @Test
    public void testAggOnGlobalWithNormalColumns2() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Only aggregate expressions allowed here");

        execStatement("select col, avg(numeric_field) from locations");
    }

    @Test
    public void testAggOnGlobalWithNormalColumns3() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Only aggregate expressions allowed here");

        execStatement("select min(bla), col, avg(numeric_field) from locations");
    }

    @Test
    public void testGroupByAnyOnObject() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Invalid column type 'object' for aggregate function ANY");
        execStatement("select any(object_field) from locations");
    }

    @Test
    public void testNoAvgDistinct() throws Exception {
        expectedException.expect(SQLParseException.class);
        execStatement("select avg(distinct age) from locations group by stuff");
    }

    @Test
    public void testNoSumDistinct() throws Exception {
        expectedException.expect(SQLParseException.class);
        execStatement("select sum(distinct age) from locations group by stuff");
    }

    @Test
    public void testNoMinDistinct() throws Exception {
        expectedException.expect(SQLParseException.class);
        execStatement("select min(distinct age) from locations group by stuff");
    }

    @Test
    public void testNoMaxDistinct() throws Exception {
        expectedException.expect(SQLParseException.class);
        execStatement("select max(distinct age) from locations group by stuff");
    }

    @Test
    public void testCountStar() throws Exception {
        execStatement("select count(*) from locations");
        assertEquals(1, stmt.aggregateExpressions().size());
        assertEquals("COUNT(*)", stmt.aggregateExpressions().get(0).functionName);
        assertNull(stmt.aggregateExpressions().get(0).expression);
        assertFalse(stmt.aggregateExpressions().get(0).isDistinct);
        assertTrue(stmt.countRequest());
    }

    @Test
    public void testCountColumn() throws Exception {
        execStatement("select count(col) from locations order by count(col)");
        assertEquals(1, stmt.aggregateExpressions().size());
        assertEquals("COUNT", stmt.aggregateExpressions().get(0).functionName);
        assertEquals("col", stmt.aggregateExpressions().get(0).expression.toString());
        assertFalse(stmt.aggregateExpressions().get(0).isDistinct);
        assertFalse(stmt.countRequest());
    }

    @Test
    public void testCountDistinct() throws Exception {
        execStatement("select count(distinct col) from locations order by count(distinct col)");
        assertEquals(1, stmt.aggregateExpressions().size());
        assertEquals("COUNT_DISTINCT", stmt.aggregateExpressions().get(0).functionName);
        assertEquals("col", stmt.aggregateExpressions().get(0).expression.toString());
        assertTrue(stmt.aggregateExpressions().get(0).isDistinct);
        assertFalse(stmt.countRequest());
    }

    @Test
    public void countOnNonExistingColumn() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Unknown column 'nothing'");
        execStatement("select count(nothing) from locations");
    }

    @Test
    public void countDistinctOnNonExistingColumn() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Unknown column 'nothing'");
        execStatement("select count(distinct nothing) from locations");
    }

    @Test
    public void selectWithTablePrefix() throws Exception {
        execStatement("select locations.col from locations where locations.col is not null order by locations.col");
        assertEquals("col", stmt.outputFields().get(0).v1());
        assertEquals("col", stmt.outputFields().get(0).v2());
        assertEquals("{\"fields\":[\"col\"]," +
                "\"query\":{\"bool\":{\"must_not\":{\"filtered\":{\"filter\":" +
                "{\"missing\":{\"field\":\"col\",\"existence\":true,\"null_value\":true}}}}}}," +
                "\"sort\":[{\"col\":{\"order\":\"asc\",\"ignore_unmapped\":true}}]," +
                "\"size\":10000}", getSource());
        assertEquals("col", stmt.orderByColumns.get(0).name);
    }

    @Test
    public void selectGroupByWithTablePrefix() throws Exception {
        execStatement("select locations.col from locations group by locations.col");
        assertEquals("col", stmt.groupByExpressions().get(0).toString());
    }

    @Test
    public void errorOnDifferentSchemaInResultColumn() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Cannot reference column from different schema.");
        execStatement("select scheme.locations.col from locations");
    }

    @Test
    public void errorOnDifferentSchemaInWhereClause() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Cannot reference column from different schema.");
        execStatement("select col from locations where scheme.location.col is not null");
    }

    @Test
    public void errorOnDifferentSchemaInWhereClauseWhereIn() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Cannot reference column from different schema.");
        execStatement("select col from locations where scheme.location.col in (1,2,3)");
    }

    @Test
    public void errorOnDifferentSchemaInOrderby() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Cannot reference column from different schema.");
        execStatement("select col from locations order by scheme.locations.wrong asc");
    }

    @Test
    public void selectGroupByWithDifferentSchemaPrefix() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Cannot reference column from different schema.");
        execStatement("select locations.col from locations group by scheme.locations.col");

    }

    @Test
    public void errorOnDifferentTableInResultColumn() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Cannot reference column from different table.");
        execStatement("select something.wrong, col from locations");
    }

    @Test
    public void errorOnDifferentTableInResultColumn2() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Cannot reference column from different table.");
        execStatement("select object_field.wrong, col from locations");
    }

    @Test
    public void errorOnDifferentTableInWhereClause() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Cannot reference column from different table.");
        execStatement("select col from locations where something.wrong = 1");
    }

    @Test
    public void errorOnDifferentTableInWhereClauseWhereIn() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Cannot reference column from different table.");
        execStatement("select col from locations where something.wrong in (1,2,3)");
    }

    @Test
    public void errorOnDifferentTableInOrderby() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Cannot reference column from different table.");
        execStatement("select col from locations order by something.wrong asc");
    }

    @Test
    public void errorOnUpdateOnDifferentTable() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Cannot reference column from different table.");
        execStatement("update locations set something.wrong='haha'");
    }

    @Test
    public void errorOnDeleteFromDifferentTable() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Cannot reference column from different table.");
        execStatement("delete from locations where something.wrong='haha'");
    }

}
