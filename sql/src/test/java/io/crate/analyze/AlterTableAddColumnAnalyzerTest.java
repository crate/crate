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

package io.crate.analyze;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import io.crate.common.collections.Maps;
import io.crate.data.Row;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.metadata.ColumnIdent;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.AlterTableAddColumnPlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.parser.ParsingException;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.crate.testing.TestingHelpers.mapToSortedString;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

public class AlterTableAddColumnAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private PlannerContext plannerContext;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .addTable("create table nested_pks (" +
                      "     pk object as (a int, b object as (c int))," +
                      "     primary key (pk['a'], pk['b']['c'])" +
                      ")")
            .build();
        plannerContext = e.getPlannerContext(clusterService.state());
    }


    private BoundAddColumn analyze(String stmt) {
        return AlterTableAddColumnPlan.bind(
            e.analyze(stmt),
            plannerContext.transactionContext(),
            plannerContext.functions(),
            Row.EMPTY,
            SubQueryResults.EMPTY,
            e.fulltextAnalyzerResolver()
        );
    }

    @Test
    public void test_can_add_column_to_table_with_multiple_nested_pks() {
        BoundAddColumn boundAddColumn = analyze("alter table nested_pks add x int");
        assertThat(
            boundAddColumn.mapping().toString(),
            is("{_meta={primary_keys=[pk.a, pk.b.c]}, " +
               "properties={" +
                    "x={position=2, type=integer}, " +
                    "pk={dynamic=true, type=object, properties={a={type=integer}, b={dynamic=true, type=object, properties={c={type=integer}}}}}}}")
        );
    }

    @Test
    public void testAddColumnOnSystemTableIsNotAllowed() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"sys.shards\" doesn't support or allow ALTER " +
                                        "operations, as it is read-only.");
        e.analyze("alter table sys.shards add column foobar string");
    }

    @Test
    public void testAddColumnOnSinglePartitionNotAllowed() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Adding a column to a single partition is not supported");
        e.analyze("alter table parted partition (date = 1395874800000) add column foobar string");
    }

    @Test
    public void testAddColumnWithAnalyzerAndNonStringType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "Can't use an Analyzer on column foobar['age'] because analyzers are only allowed on columns of type \"string\"");
        analyze("alter table users add column foobar object as (age int index using fulltext)");
    }

    @Test
    public void testAddFulltextIndex() throws Exception {
        expectedException.expect(ParsingException.class);
        e.analyze("alter table users add column index ft_foo using fulltext (name)");
    }

    @Test
    public void testAddColumnThatExistsAlready() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The table doc.users already has a column named name");
        analyze("alter table users add column name string");
    }

    @Test
    public void testAddColumnToATableWithoutPrimaryKey() throws Exception {
        BoundAddColumn analysis = analyze(
            "alter table users_clustered_by_only add column foobar string");
        Map<String, Object> mapping = analysis.mapping();

        Object primaryKeys = ((Map) mapping.get("_meta")).get("primary_keys");
        assertNull(primaryKeys); // _id shouldn't be included
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testAddColumnAsPrimaryKey() throws Exception {
        BoundAddColumn analysis = analyze(
            "alter table users add column additional_pk string primary key");

        assertThat(AnalyzedTableElements.primaryKeys(analysis.analyzedTableElements()), Matchers.contains(
            "additional_pk", "id"
        ));

        AnalyzedColumnDefinition<Object> idColumn = null;
        AnalyzedColumnDefinition<Object> additionalPkColumn = null;
        for (AnalyzedColumnDefinition<Object> column : analysis.analyzedTableElements().columns()) {
            if (column.name().equals("id")) {
                idColumn = column;
            } else {
                additionalPkColumn = column;
            }
        }
        assertNotNull(idColumn);
        assertThat(idColumn.ident(), equalTo(new ColumnIdent("id")));
        assertThat(idColumn.dataType(), equalTo(DataTypes.LONG));

        assertNotNull(additionalPkColumn);
        assertThat(additionalPkColumn.ident(), equalTo(new ColumnIdent("additional_pk")));
        assertThat(additionalPkColumn.dataType(), equalTo(DataTypes.STRING));
    }

    @Test
    public void testAddPrimaryKeyColumnWithArrayTypeUnsupported() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot use columns of type \"array\" as primary key");
        analyze("alter table users add column newpk array(string) primary key");
    }

    @Test
    public void testAddColumnToATableWithNotNull() throws Exception {
        BoundAddColumn analysis = analyze("alter table users_clustered_by_only " +
                                          "add column notnullcol string not null");
        Map<String, Object> mapping = analysis.mapping();

        assertThat((String) ((Set) ((Map) ((Map) mapping.get("_meta")).get("constraints")).get("not_null"))
            .toArray(new String[0])[0], is("notnullcol"));
    }

    @Test
    public void testAddArrayColumn() throws Exception {
        BoundAddColumn analysis = analyze("alter table users add newtags array(string)");
        AnalyzedColumnDefinition<Object> columnDefinition = analysis.analyzedTableElements().columns().get(0);
        assertThat(columnDefinition.name(), Matchers.equalTo("newtags"));
        assertThat(columnDefinition.dataType(), Matchers.equalTo(DataTypes.STRING));
        assertTrue(columnDefinition.isArrayOrInArray());

        Map<String, Object> mappingProperties = (Map) analysis.mapping().get("properties");
        Map<String, Object> newtags = (Map<String, Object>) mappingProperties.get("newtags");

        assertThat((String) newtags.get("type"), is("array"));
        Map<String, Object> inner = (Map<String, Object>) newtags.get("inner");
        assertThat((String) inner.get("type"), is("keyword"));
    }

    public void testAddObjectColumnWithUnderscore() throws Exception {
        BoundAddColumn analysis = analyze("alter table users add column foo['_x'] int");

        assertThat(analysis.analyzedTableElements().columns().size(), is(2)); // id pk column is also added
        AnalyzedColumnDefinition<Object> column = analysis.analyzedTableElements().columns().get(0);
        assertThat(column.ident(), Matchers.equalTo(new ColumnIdent("foo")));
        assertThat(column.children().size(), is(1));
        AnalyzedColumnDefinition<Object> xColumn = column.children().get(0);
        assertThat(xColumn.ident(), Matchers.equalTo(new ColumnIdent("foo", Arrays.asList("_x"))));
    }

    @Test
    public void testAddNewNestedObjectColumn() throws Exception {
        BoundAddColumn analysis = analyze(
            "alter table users add column foo['x']['y'] string");

        assertThat(analysis.analyzedTableElements().columns().size(), is(2)); // id pk column is also added
        AnalyzedColumnDefinition<Object> column = analysis.analyzedTableElements().columns().get(0);
        assertThat(column.ident(), Matchers.equalTo(new ColumnIdent("foo")));
        assertThat(column.children().size(), is(1));
        AnalyzedColumnDefinition<Object> xColumn = column.children().get(0);
        assertThat(xColumn.ident(), Matchers.equalTo(new ColumnIdent("foo", Arrays.asList("x"))));
        assertThat(xColumn.children().size(), is(1));
        AnalyzedColumnDefinition<Object> yColumn = xColumn.children().get(0);
        assertThat(yColumn.ident(), Matchers.equalTo(new ColumnIdent("foo", Arrays.asList("x", "y"))));
        assertThat(yColumn.children().size(), is(0));

        Map<String, Object> mapping = analysis.mapping();
        Map foo = (Map) Maps.getByPath(mapping, "properties.foo");
        assertThat((String) foo.get("type"), is("object"));

        Map x = (Map) Maps.getByPath(mapping, "properties.foo.properties.x");
        assertThat((String) x.get("type"), is("object"));

        Map y = (Map) Maps.getByPath(mapping, "properties.foo.properties.x.properties.y");
        assertThat((String) y.get("type"), is("keyword"));
    }

    @Test
    public void testAddNewNestedColumnToObjectArray() throws Exception {
        BoundAddColumn analysis = analyze("alter table users add friends['is_nice'] BOOLEAN");

        List<AnalyzedColumnDefinition<Object>> columns = analysis.analyzedTableElements().columns();
        assertThat(columns.size(), is(2)); // second one is primary key
        AnalyzedColumnDefinition<Object> friends = columns.get(0);
        assertThat(mapToSortedString(AnalyzedColumnDefinition.toMapping(friends)), is("inner={" +
                                                                "dynamic=true, " +
                                                                "position=10, " +
                                                                "properties={" +
                                                                    "is_nice={type=boolean}" +
                                                                "}, " +
                                                                "type=object" +
                                                              "}, type=array"));
    }

    @Test
    public void testAddColumnToObjectTypeMaintainsObjectPolicy() throws Exception {
        BoundAddColumn analysis = analyze(
            "alter table users add column address['street'] string");
        List<AnalyzedColumnDefinition<Object>> columns = analysis.analyzedTableElements().columns();
        assertThat(columns.size(), is(2));

        AnalyzedColumnDefinition<Object> address = columns.get(0);
        assertThat(address.objectType, is(ColumnPolicy.STRICT));
    }

    @Test
    public void testAddColumnToStrictObject() {
        BoundAddColumn analysis = analyze(
            "alter table users add column address['street'] string");
        List<AnalyzedColumnDefinition<Object>> columns = analysis.analyzedTableElements().columns();
        assertThat(columns.size(), is(2));

        AnalyzedColumnDefinition<Object> address = columns.get(0);
        AnalyzedColumnDefinition<Object> street = address.children().get(0);
        assertThat(street.ident(), is(ColumnIdent.fromPath("address.street")));
        assertThat(street.dataType(), is(DataTypes.STRING));
        assertThat(street.isParentColumn(), is(false));
    }

    @Test
    public void testAddNewNestedColumnToObjectColumn() throws Exception {
        BoundAddColumn analysis = analyze(
            "alter table users add column details['foo'] object as (score float, name string)");
        List<AnalyzedColumnDefinition<Object>> columns = analysis.analyzedTableElements().columns();
        assertThat(columns.size(), is(2)); // second one is primary key

        AnalyzedColumnDefinition<Object> details = columns.get(0);
        assertThat(details.ident(), is(ColumnIdent.fromPath("details")));
        assertThat(details.dataType().id(), is(ObjectType.ID));
        assertThat(details.isParentColumn(), is(true));
        assertThat(details.children().size(), is(1));

        AnalyzedColumnDefinition<Object> foo = details.children().get(0);
        assertThat(foo.ident(), is(ColumnIdent.fromPath("details.foo")));
        assertThat(foo.dataType().id(), is(ObjectType.ID));
        assertThat(foo.isParentColumn(), is(false));

        assertThat(columns.get(0).children().get(0).children().size(), is(2));

        AnalyzedColumnDefinition<Object> score = columns.get(0).children().get(0).children().get(0);
        assertThat(score.ident(), is(ColumnIdent.fromPath("details.foo.score")));
        assertThat(score.dataType(), is(DataTypes.FLOAT));

        AnalyzedColumnDefinition name = columns.get(0).children().get(0).children().get(1);
        assertThat(name.ident(), is(ColumnIdent.fromPath("details.foo.name")));
        assertThat(name.dataType(), is(DataTypes.STRING));

        Map<String, Object> mapping = analysis.mapping();
        assertThat(mapToSortedString(mapping),
            is("_meta={primary_keys=[id]}, properties={details={dynamic=true, position=6, " +
               "properties={foo={dynamic=true, properties={name={type=keyword}, score={type=float}}, type=object}}, " +
               "type=object}, id={type=long}}"));
    }

    @Test
    public void testAddNewNestedColumnWithArrayToRoot() throws Exception {
        BoundAddColumn analysis = analyze(
            "alter table users add column new_obj_col object as (a array(long))");
        List<AnalyzedColumnDefinition<Object>> columns = analysis.analyzedTableElements().columns();
        assertThat(columns.size(), is(2)); // second one is primary key
        assertThat(columns.get(0).dataType().id(), is(ObjectType.ID));
        assertThat(columns.get(0).children().get(0).dataType(), is(DataTypes.LONG));
        assertTrue(columns.get(0).children().get(0).isArrayOrInArray());
    }

    @Test
    public void testAddNewNestedColumnWithArrayToObjectColumn() throws Exception {
        BoundAddColumn analysis = analyze(
            "alter table users add column new_obj_col object as (o object as (b array(long)))");
        List<AnalyzedColumnDefinition<Object>> columns = analysis.analyzedTableElements().columns();
        assertThat(columns.size(), is(2)); // second one is primary key
        assertThat(columns.get(0).children().get(0).dataType().id(), is(ObjectType.ID));
        assertThat(columns.get(0).children().get(0).children().get(0).dataType(), is(DataTypes.LONG));
        assertTrue(columns.get(0).children().get(0).children().get(0).isArrayOrInArray());
    }

    @Test
    public void testAddNewNestedColumnToNestedObjectColumn() throws Exception {
        BoundAddColumn analysis = analyze(
            "alter table deeply_nested add column details['stuff']['foo'] object as (score float, price string)");
        List<AnalyzedColumnDefinition<Object>> columns = analysis.analyzedTableElements().columns();
        assertThat(columns.size(), is(1));
        assertThat(columns.get(0).ident(), is(ColumnIdent.fromPath("details")));
        assertThat(columns.get(0).dataType().id(), is(ObjectType.ID));
        assertThat(columns.get(0).isParentColumn(), is(true));
        assertThat(columns.get(0).children().size(), is(1));

        AnalyzedColumnDefinition<Object> stuff = columns.get(0).children().get(0);
        assertThat(stuff.ident(), is(ColumnIdent.fromPath("details.stuff")));
        assertThat(stuff.dataType().id(), is(ObjectType.ID));
        assertThat(stuff.isParentColumn(), is(true));
        assertThat(stuff.children().size(), is(1));

        AnalyzedColumnDefinition<Object> foo = stuff.children().get(0);
        assertThat(foo.ident(), is(ColumnIdent.fromPath("details.stuff.foo")));
        assertThat(foo.dataType().id(), is(ObjectType.ID));
        assertThat(foo.isParentColumn(), is(false));
        assertThat(foo.children().size(), is(2));

        AnalyzedColumnDefinition<Object> score = foo.children().get(0);
        assertThat(score.ident(), is(ColumnIdent.fromPath("details.stuff.foo.score")));
        assertThat(score.dataType(), is(DataTypes.FLOAT));

        AnalyzedColumnDefinition<Object> price = foo.children().get(1);
        assertThat(price.ident(), is(ColumnIdent.fromPath("details.stuff.foo.price")));
        assertThat(price.dataType(), is(DataTypes.STRING));

        Map<String, Object> mapping = analysis.mapping();
        assertThat(mapToSortedString(mapping),
            is("_meta={}, properties={details={dynamic=true, position=1, " +
               "properties={stuff={dynamic=true, properties={foo={dynamic=true, properties={price={type=keyword}, " +
               "score={type=float}}, type=object}}, type=object}}, type=object}}"));
    }

    @Test
    public void testAddGeneratedColumn() throws Exception {
        BoundAddColumn analysis = analyze(
            "alter table users add column name_generated as concat(name, 'foo')");

        assertThat(analysis.hasNewGeneratedColumns(), is(true));
        assertThat(analysis.analyzedTableElements().columnIdents(), containsInAnyOrder(
            new ColumnIdent("name_generated"), new ColumnIdent("id")));

        AnalyzedColumnDefinition nameGeneratedColumn = null;
        for (AnalyzedColumnDefinition<Object> columnDefinition : analysis.analyzedTableElements().columns()) {
            if (columnDefinition.ident().name().equals("name_generated")) {
                nameGeneratedColumn = columnDefinition;
            }
        }
        assertNotNull(nameGeneratedColumn);
        assertThat(nameGeneratedColumn.dataType(), equalTo(DataTypes.STRING));
        assertThat(nameGeneratedColumn.formattedGeneratedExpression(), is("concat(name, 'foo')"));
    }

    @Test
    public void testAddColumnWithCheckConstraint() throws Exception {
        BoundAddColumn analysis = analyze(
            "alter table users add column bazinga int constraint bazinga_check check(bazinga > 0)");
        assertThat(analysis.analyzedTableElements().getCheckConstraints(), is(Map.of("bazinga_check", "\"bazinga\" > 0")));
        Map<String, Object> mapping = analysis.mapping();
        assertThat(mapToSortedString(mapping),
                   is("_meta={check_constraints={bazinga_check=\"bazinga\" > 0}, primary_keys=[id]}, " +
                      "properties={bazinga={position=18, type=integer}, id={type=long}}"));
    }

    @Test
    public void testAddColumnWithCheckConstraintFailsBecauseItRefersToAnotherColumn() throws Exception {
        expectedException.expectMessage("CHECK expressions defined in this context cannot refer to other columns: id");
        BoundAddColumn analysis = analyze(
            "alter table users add column bazinga int constraint bazinga_check check(id > 0)");
    }

    @Test
    public void testAddColumnWithColumnStoreDisabled() throws Exception {
        BoundAddColumn analysis = analyze(
            "alter table users add column string_no_docvalues string STORAGE WITH (columnstore = false)");
        Map<String, Object> mapping = analysis.mapping();
        assertThat(mapToSortedString(mapping),
            is("_meta={primary_keys=[id]}, properties={id={type=long}, " +
               "string_no_docvalues={doc_values=false, position=18, type=keyword}}"));
    }
}
