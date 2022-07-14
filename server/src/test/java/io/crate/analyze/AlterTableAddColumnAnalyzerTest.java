/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import static io.crate.testing.TestingHelpers.mapToSortedString;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hamcrest.Matchers;
import org.junit.Test;

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

public class AlterTableAddColumnAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    private BoundAddColumn analyze(String stmt) {
        PlannerContext plannerContext = e.getPlannerContext(clusterService.state());
        return AlterTableAddColumnPlan.bind(
            e.analyze(stmt),
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            Row.EMPTY,
            SubQueryResults.EMPTY,
            e.fulltextAnalyzerResolver()
        );
    }

    @Test
    public void test_can_add_column_to_table_with_multiple_nested_pks() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table nested_pks (" +
                      "     pk object as (a int, b object as (c int))," +
                      "     primary key (pk['a'], pk['b']['c'])" +
                      ")")
            .build();
        BoundAddColumn boundAddColumn = analyze("alter table nested_pks add x int");
        assertThat(
            boundAddColumn.mapping().toString(),
            is("{_meta={primary_keys=[pk.a, pk.b.c]}, " +
               "properties={" +
                    "x={position=-1, type=integer}, " +
                    "pk={dynamic=true, position=1, type=object, " +
                        "properties={a={position=2, type=integer}, " +
                            "b={dynamic=true, position=3, type=object, properties={c={position=4, type=integer}}}}}}}")
        );
    }

    @Test
    public void testAddColumnOnSystemTableIsNotAllowed() throws Exception {
        e = SQLExecutor.builder(clusterService).build();

        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"sys.shards\" doesn't support or allow ALTER " +
                                        "operations, as it is read-only.");
        e.analyze("alter table sys.shards add column foobar string");
    }

    @Test
    public void testAddColumnOnSinglePartitionNotAllowed() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addPartitionedTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS)
            .build();

        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Adding a column to a single partition is not supported");
        e.analyze("alter table parted partition (date = 1395874800000) add column foobar string");
    }

    @Test
    public void testAddColumnWithAnalyzerAndNonStringType() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (name text)")
            .build();

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "Can't use an Analyzer on column foobar['age'] because analyzers are only allowed " +
            "on columns of type \"" + DataTypes.STRING.getName() + "\" of the unbound length limit");
        analyze("alter table users add column foobar object as (age int index using fulltext)");
    }

    @Test
    public void testAddFulltextIndex() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (name text)")
            .build();

        expectedException.expect(ParsingException.class);
        e.analyze("alter table users add column index ft_foo using fulltext (name)");
    }

    @Test
    public void testAddColumnThatExistsAlready() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (name text)")
            .build();

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The table doc.users already has a column named name");
        analyze("alter table users add column name string");
    }

    @Test
    public void testAddColumnToATableWithoutPrimaryKey() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (id bigint, name text) clustered by (id)")
            .build();
        BoundAddColumn analysis = analyze(
            "alter table users add column foobar string");
        Map<String, Object> mapping = analysis.mapping();

        Object primaryKeys = ((Map) mapping.get("_meta")).get("primary_keys");
        assertNull(primaryKeys); // _id shouldn't be included
    }

    @Test
    public void testAddColumnAsPrimaryKey() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (id bigint primary key, name text)")
            .build();
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
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (id bigint primary key, name text)")
            .build();

        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot use columns of type \"array\" as primary key");
        analyze("alter table users add column newpk array(string) primary key");
    }

    @Test
    public void testAddColumnToATableWithNotNull() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (id bigint, name text) clustered by (id)")
            .build();

        BoundAddColumn analysis = analyze(
            "alter table users add column notnullcol string not null");
        Map<String, Object> mapping = analysis.mapping();

        assertThat((String) ((Set) ((Map) ((Map) mapping.get("_meta")).get("constraints")).get("not_null"))
            .toArray(new String[0])[0], is("notnullcol"));
    }

    @Test
    public void testAddArrayColumn() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (id bigint primary key, name text)")
            .build();

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
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (id bigint primary key, name text)")
            .build();
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
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (id bigint primary key, name text)")
            .build();
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
        e = SQLExecutor.builder(clusterService)
            .addTable("""
                create table users (
                    id bigint primary key,
                    friends array(object as (
                        id long,
                        groups array(string)
                    ))
                )
            """)
            .build();
        BoundAddColumn analysis = analyze("alter table users add friends['is_nice'] BOOLEAN");

        List<AnalyzedColumnDefinition<Object>> columns = analysis.analyzedTableElements().columns();
        assertThat(columns.size(), is(2)); // second one is primary key
        AnalyzedColumnDefinition<Object> friends = columns.get(0);
        assertThat(mapToSortedString(AnalyzedColumnDefinition.toMapping(friends)), is("inner={" +
                                                                "dynamic=true, " +
                                                                "position=2, " +
                                                                "properties={" +
                                                                    "is_nice={" +
                                                                      "position=-1, type=boolean" +
                                                                    "}" +
                                                                "}, " +
                                                                "type=object" +
                                                              "}, type=array"));
    }

    @Test
    public void testAddColumnToObjectTypeMaintainsObjectPolicy() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("""
                create table users (
                    id bigint primary key,
                    address object (strict) as (
                        postcode text
                    )
                )
            """)
            .build();
        BoundAddColumn analysis = analyze(
            "alter table users add column address['street'] string");
        List<AnalyzedColumnDefinition<Object>> columns = analysis.analyzedTableElements().columns();
        assertThat(columns.size(), is(2));

        AnalyzedColumnDefinition<Object> address = columns.get(0);
        assertThat(address.objectType, is(ColumnPolicy.STRICT));
    }

    @Test
    public void testAddColumnToStrictObject() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("""
                create table users (
                    id bigint primary key,
                    address object (strict) as (
                        postcode text
                    )
                )
            """)
            .build();
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
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (id bigint primary key, details object)")
            .build();
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
            is("_meta={primary_keys=[id]}, " +
               "properties={details={dynamic=true, position=2," +
               " properties={foo={dynamic=true, position=-1," +
               " properties={name={position=-3, type=keyword}, score={position=-2, type=float}}, type=object}}," +
               " type=object}, " +
               "id={position=1, type=long}}"));
    }

    @Test
    public void testAddNewNestedColumnWithArrayToRoot() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (id bigint primary key)")
            .build();
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
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (id bigint primary key)")
            .build();
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
        e = SQLExecutor.builder(clusterService)
            .addTable(TableDefinitions.DEEPLY_NESTED_TABLE_DEFINITION)
            .build();
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
               "properties={stuff={dynamic=true, position=3, properties={foo={dynamic=true, position=-1, " +
               "properties={price={position=-3, type=keyword}, " +
               "score={position=-2, type=float}}, type=object}}, type=object}}, type=object}}"));
    }

    @Test
    public void testAddGeneratedColumn() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (id bigint primary key, name text)")
            .build();
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
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (id bigint primary key, name text)")
            .build();
        BoundAddColumn analysis = analyze(
            "alter table users add column bazinga int constraint bazinga_check check(bazinga > 0)");
        assertThat(analysis.analyzedTableElements().getCheckConstraints(), is(Map.of("bazinga_check", "\"bazinga\" > 0")));
        Map<String, Object> mapping = analysis.mapping();
        assertThat(mapToSortedString(mapping),
                   is("_meta={check_constraints={bazinga_check=\"bazinga\" > 0}, primary_keys=[id]}, " +
                      "properties={bazinga={position=-1, type=integer}, id={position=1, type=long}}"));
    }

    @Test
    public void testAddColumnWithCheckConstraintFailsBecauseItRefersToAnotherColumn() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (id bigint primary key, name text)")
            .build();
        expectedException.expectMessage("CHECK expressions defined in this context cannot refer to other columns: id");
        BoundAddColumn analysis = analyze(
            "alter table users add column bazinga int constraint bazinga_check check(id > 0)");
    }

    @Test
    public void testAddColumnWithColumnStoreDisabled() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (id bigint primary key, name text)")
            .build();
        BoundAddColumn analysis = analyze(
            "alter table users add column string_no_docvalues string STORAGE WITH (columnstore = false)");
        Map<String, Object> mapping = analysis.mapping();
        assertThat(mapToSortedString(mapping),
            is("_meta={primary_keys=[id]}, properties={id={position=1, type=long}, " +
               "string_no_docvalues={doc_values=false, position=-1, type=keyword}}"));
    }

    @Test
    public void test_primary_key_contains_index_definitions_on_alter_table_new_column() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (num bigint index off, primary key (num))")
            .build();
        BoundAddColumn addColumn = analyze("alter table tbl add column browser text");
        Map<String, Object> mapping = (Map<String, Object>) addColumn.mapping().get("properties");
        assertThat(mapping, Matchers.hasEntry(is("num"), is(Map.of("index", false, "position", 1, "type", "long"))));
    }

    @Test
    public void test_adding_a_column_with_constraint_adds_existing_constraints_in_mapping() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE tbl (author text CHECK (author != ''))")
            .build();
        BoundAddColumn addColumn = analyze("ALTER TABLE tbl ADD COLUMN dummy text CHECK (dummy != '')");
        Map<String, Object> mapping = (Map<String, Object>) addColumn.mapping();
        Map<String, Object> meta = (Map<String, Object>) mapping.get("_meta");
        Map<String, Object> checks = (Map<String, Object>) meta.get("check_constraints");

        // not asserting concrete keys because check names contain random suffixes
        assertThat(checks.size(), is(2));
    }

    @Test
    public void test_adding_not_null_column_only_contains_new_column_names_in_constraints() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE tbl (author text NOT NULL)")
            .build();
        BoundAddColumn addColumn = analyze("ALTER TABLE tbl ADD COLUMN dummy text NOT NULL");
        Map<String, Object> mapping = (Map<String, Object>) addColumn.mapping();
        Map<String, Object> meta = (Map<String, Object>) mapping.get("_meta");
        Collection<String> notNull = (Collection<String>)
            ((Map<String, Object>) meta.get("constraints")).get("not_null");

        assertThat(notNull, Matchers.containsInAnyOrder("dummy"));
    }

    @Test
    public void test_can_add_fulltext_columns_without_explicit_analyzer() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE tbl (x int)")
            .build();
        BoundAddColumn addColumn = analyze("ALTER TABLE tbl ADD COLUMN content TEXT INDEX USING FULLTEXT");
        Map<String, Object> properties = (Map<String, Object>) addColumn.mapping().get("properties");
        Map<String, Object> content = (Map<String, Object>) properties.get("content");

        assertThat(
            "Fulltext columns must have type `text`. Regular varchar or text columns have type `keyword`",
            content.get("type"),
            is("text")
        );
    }

    @Test
    public void test_primary_key_varchar_length_is_not_lost_after_altering_table() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE tbl1 (x VARCHAR(512), PRIMARY KEY(x))")
            .addTable("CREATE TABLE tbl2 (x BIT(10), PRIMARY KEY(x))")
            .build();
        BoundAddColumn addColumn = analyze("ALTER TABLE tbl1 ADD COLUMN y DOUBLE");
        Map<String, Object> properties = (Map<String, Object>) addColumn.mapping().get("properties");
        Map<String, Object> pk = (Map<String, Object>) properties.get("x");

        assertThat(
            "VARCHAR length info must be retained while mirroring PK mapping",
            pk.get("length_limit"),
            is(512)
        );

        addColumn = analyze("ALTER TABLE tbl2 ADD COLUMN y DOUBLE");
        properties = (Map<String, Object>) addColumn.mapping().get("properties");
        pk = (Map<String, Object>) properties.get("x");

        assertThat(
            "BIT length info must be retained while mirroring PK mapping",
            pk.get("length"),
            is(10)
        );
    }

    @Test
    public void test_default_column_varchar_length_of_the_expression_is_not_lost_after_altering_table() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE tbl (x int)")
            .build();

        BoundAddColumn addColumn = analyze("ALTER TABLE tbl ADD COLUMN gen generated always as '123'::varchar(10)");
        Map<String, Object> properties = (Map<String, Object>) addColumn.mapping().get("properties");
        Map<String, Object> gen = (Map<String, Object>) properties.get("gen");

        assertThat(
            "VARCHAR length info must be retained while processing generated column",
            gen.get("length_limit"),
            is(10)
        );
    }
}
