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

import io.crate.core.collections.StringObjectMaps;
import io.crate.metadata.ColumnIdent;
import io.crate.sql.parser.ParsingException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

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

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testAddColumnOnSystemTableIsNotAllowed() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
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
        e.analyze("alter table users add column foobar object as (age int index using fulltext)");
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
        e.analyze("alter table users add column name string");
    }

    @Test
    public void testAddColumnToATableWithoutPrimaryKey() throws Exception {
        AddColumnAnalyzedStatement analysis = e.analyze(
            "alter table users_clustered_by_only add column foobar string");
        Map<String, Object> mapping = analysis.analyzedTableElements().toMapping();

        Object primaryKeys = ((Map) mapping.get("_meta")).get("primary_keys");
        assertNull(primaryKeys); // _id shouldn't be included
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testAddColumnAsPrimaryKey() throws Exception {
        AddColumnAnalyzedStatement analysis = e.analyze(
            "alter table users add column additional_pk string primary key");

        assertThat(analysis.analyzedTableElements().primaryKeys(), Matchers.contains(
            "additional_pk", "id"
        ));

        AnalyzedColumnDefinition idColumn = null;
        AnalyzedColumnDefinition additionalPkColumn = null;
        for (AnalyzedColumnDefinition column : analysis.analyzedTableElements().columns()) {
            if (column.name().equals("id")) {
                idColumn = column;
            } else {
                additionalPkColumn = column;
            }
        }
        assertNotNull(idColumn);
        assertThat(idColumn.ident(), equalTo(new ColumnIdent("id")));
        assertThat(idColumn.dataType(), equalTo("long"));

        assertNotNull(additionalPkColumn);
        assertThat(additionalPkColumn.ident(), equalTo(new ColumnIdent("additional_pk")));
        assertThat(additionalPkColumn.dataType(), equalTo("string"));
    }

    @Test
    public void testAddPrimaryKeyColumnWithArrayTypeUnsupported() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot use columns of type \"array\" as primary key");
        e.analyze("alter table users add column newpk array(string) primary key");
    }

    @Test
    public void testAddColumnToATableWithNotNull() throws Exception {
        AddColumnAnalyzedStatement analysis = e.analyze("alter table users_clustered_by_only " +
                                                        "add column notnullcol string not null");
        Map<String, Object> mapping = analysis.analyzedTableElements().toMapping();

        assertThat((String) ((Set) ((Map) ((Map) mapping.get("_meta")).get("constraints")).get("not_null"))
            .toArray(new String[0])[0], is("notnullcol"));
    }

    @Test
    public void testAddArrayColumn() throws Exception {
        AddColumnAnalyzedStatement analysis = e.analyze("alter table users add newtags array(string)");
        AnalyzedColumnDefinition columnDefinition = analysis.analyzedTableElements().columns().get(0);
        assertThat(columnDefinition.name(), Matchers.equalTo("newtags"));
        assertThat(columnDefinition.dataType(), Matchers.equalTo("string"));
        assertTrue(columnDefinition.isArrayOrInArray());

        Map<String, Object> mappingProperties = (Map) analysis.analyzedTableElements().toMapping().get("properties");
        Map<String, Object> newtags = (Map<String, Object>) mappingProperties.get("newtags");

        assertThat((String) newtags.get("type"), is("array"));
        Map<String, Object> inner = (Map<String, Object>) newtags.get("inner");
        assertThat((String) inner.get("type"), is("keyword"));
    }

    public void testAddObjectColumnWithUnderscore() throws Exception {
        AddColumnAnalyzedStatement analysis = e.analyze("alter table users add column foo['_x'] int");

        assertThat(analysis.analyzedTableElements().columns().size(), is(2)); // id pk column is also added
        AnalyzedColumnDefinition column = analysis.analyzedTableElements().columns().get(0);
        assertThat(column.ident(), Matchers.equalTo(new ColumnIdent("foo")));
        assertThat(column.children().size(), is(1));
        AnalyzedColumnDefinition xColumn = column.children().get(0);
        assertThat(xColumn.ident(), Matchers.equalTo(new ColumnIdent("foo", Arrays.asList("_x"))));
    }

    @Test
    public void testAddNewNestedObjectColumn() throws Exception {
        AddColumnAnalyzedStatement analysis = e.analyze(
            "alter table users add column foo['x']['y'] string");

        assertThat(analysis.analyzedTableElements().columns().size(), is(2)); // id pk column is also added
        AnalyzedColumnDefinition column = analysis.analyzedTableElements().columns().get(0);
        assertThat(column.ident(), Matchers.equalTo(new ColumnIdent("foo")));
        assertThat(column.children().size(), is(1));
        AnalyzedColumnDefinition xColumn = column.children().get(0);
        assertThat(xColumn.ident(), Matchers.equalTo(new ColumnIdent("foo", Arrays.asList("x"))));
        assertThat(xColumn.children().size(), is(1));
        AnalyzedColumnDefinition yColumn = xColumn.children().get(0);
        assertThat(yColumn.ident(), Matchers.equalTo(new ColumnIdent("foo", Arrays.asList("x", "y"))));
        assertThat(yColumn.children().size(), is(0));

        Map<String, Object> mapping = analysis.analyzedTableElements().toMapping();
        Map foo = (Map) StringObjectMaps.getByPath(mapping, "properties.foo");
        assertThat((String) foo.get("type"), is("object"));

        Map x = (Map) StringObjectMaps.getByPath(mapping, "properties.foo.properties.x");
        assertThat((String) x.get("type"), is("object"));

        Map y = (Map) StringObjectMaps.getByPath(mapping, "properties.foo.properties.x.properties.y");
        assertThat((String) y.get("type"), is("keyword"));
    }

    @Test
    public void testAddNewNestedColumnToObjectArray() throws Exception {
        AddColumnAnalyzedStatement analysis = e.analyze("alter table users add friends['is_nice'] BOOLEAN");

        List<AnalyzedColumnDefinition> columns = analysis.analyzedTableElements().columns();
        assertThat(columns.size(), is(2)); // second one is primary key
        AnalyzedColumnDefinition friends = columns.get(0);
        assertThat(mapToSortedString(friends.toMapping()), is("inner={" +
                                                                "dynamic=true, " +
                                                                "properties={" +
                                                                    "is_nice={type=boolean}" +
                                                                "}, " +
                                                                "type=object" +
                                                              "}, type=array"));
    }

    @Test
    public void testAddColumnToObjectTypeMaintainsObjectPolicy() throws Exception {
        AddColumnAnalyzedStatement analysis = e.analyze(
            "alter table users add column address['street'] string");
        List<AnalyzedColumnDefinition> columns = analysis.analyzedTableElements().columns();
        assertThat(columns.size(), is(2));

        AnalyzedColumnDefinition address = columns.get(0);
        assertThat(address.objectType, is("strict"));
    }

    @Test
    public void testAddColumnToStrictObject() {
        AddColumnAnalyzedStatement analysis = e.analyze(
            "alter table users add column address['street'] string");
        List<AnalyzedColumnDefinition> columns = analysis.analyzedTableElements().columns();
        assertThat(columns.size(), is(2));

        AnalyzedColumnDefinition address = columns.get(0);
        AnalyzedColumnDefinition street = address.children().get(0);
        assertThat(street.ident(), is(ColumnIdent.fromPath("address.street")));
        assertThat(street.dataType(), is("string"));
        assertThat(street.isParentColumn(), is(false));
    }

    @Test
    public void testAddNewNestedColumnToObjectColumn() throws Exception {
        AddColumnAnalyzedStatement analysis = e.analyze(
            "alter table users add column details['foo'] object as (score float, name string)");
        List<AnalyzedColumnDefinition> columns = analysis.analyzedTableElements().columns();
        assertThat(columns.size(), is(2)); // second one is primary key

        AnalyzedColumnDefinition details = columns.get(0);
        assertThat(details.ident(), is(ColumnIdent.fromPath("details")));
        assertThat(details.dataType(), is("object"));
        assertThat(details.isParentColumn(), is(true));
        assertThat(details.children().size(), is(1));

        AnalyzedColumnDefinition foo = details.children().get(0);
        assertThat(foo.ident(), is(ColumnIdent.fromPath("details.foo")));
        assertThat(foo.dataType(), is("object"));
        assertThat(foo.isParentColumn(), is(false));

        assertThat(columns.get(0).children().get(0).children().size(), is(2));

        AnalyzedColumnDefinition score = columns.get(0).children().get(0).children().get(0);
        assertThat(score.ident(), is(ColumnIdent.fromPath("details.foo.score")));
        assertThat(score.dataType(), is("float"));

        AnalyzedColumnDefinition name = columns.get(0).children().get(0).children().get(1);
        assertThat(name.ident(), is(ColumnIdent.fromPath("details.foo.name")));
        assertThat(name.dataType(), is("string"));

        Map<String, Object> mapping = analysis.analyzedTableElements().toMapping();
        assertThat(mapToSortedString(mapping),
            is("_all={enabled=false}, _meta={primary_keys=[id]}, properties={details={dynamic=true, " +
               "properties={foo={dynamic=true, properties={name={type=keyword}, score={type=float}}, type=object}}, " +
               "type=object}, id={type=long}}"));
    }

    @Test
    public void testAddNewNestedColumnWithArrayToRoot() throws Exception {
        AddColumnAnalyzedStatement analysis = e.analyze(
            "alter table users add column new_obj_col object as (a array(long))");
        List<AnalyzedColumnDefinition> columns = analysis.analyzedTableElements().columns();
        assertThat(columns.size(), is(2)); // second one is primary key
        assertThat(columns.get(0).dataType(), is("object"));
        assertThat(columns.get(0).children().get(0).dataType(), is("long"));
        assertTrue(columns.get(0).children().get(0).isArrayOrInArray());
    }

    @Test
    public void testAddNewNestedColumnWithArrayToObjectColumn() throws Exception {
        AddColumnAnalyzedStatement analysis = e.analyze(
            "alter table users add column new_obj_col object as (o object as (b array(long)))");
        List<AnalyzedColumnDefinition> columns = analysis.analyzedTableElements().columns();
        assertThat(columns.size(), is(2)); // second one is primary key
        assertThat(columns.get(0).children().get(0).dataType(), is("object"));
        assertThat(columns.get(0).children().get(0).children().get(0).dataType(), is("long"));
        assertTrue(columns.get(0).children().get(0).children().get(0).isArrayOrInArray());
    }

    @Test
    public void testAddNewNestedColumnToNestedObjectColumn() throws Exception {
        AddColumnAnalyzedStatement analysis = e.analyze(
            "alter table deeply_nested add column details['stuff']['foo'] object as (score float, price string)");
        List<AnalyzedColumnDefinition> columns = analysis.analyzedTableElements().columns();
        assertThat(columns.size(), is(1));
        assertThat(columns.get(0).ident(), is(ColumnIdent.fromPath("details")));
        assertThat(columns.get(0).dataType(), is("object"));
        assertThat(columns.get(0).isParentColumn(), is(true));
        assertThat(columns.get(0).children().size(), is(1));

        AnalyzedColumnDefinition stuff = columns.get(0).children().get(0);
        assertThat(stuff.ident(), is(ColumnIdent.fromPath("details.stuff")));
        assertThat(stuff.dataType(), is("object"));
        assertThat(stuff.isParentColumn(), is(true));
        assertThat(stuff.children().size(), is(1));

        AnalyzedColumnDefinition foo = stuff.children().get(0);
        assertThat(foo.ident(), is(ColumnIdent.fromPath("details.stuff.foo")));
        assertThat(foo.dataType(), is("object"));
        assertThat(foo.isParentColumn(), is(false));
        assertThat(foo.children().size(), is(2));

        AnalyzedColumnDefinition score = foo.children().get(0);
        assertThat(score.ident(), is(ColumnIdent.fromPath("details.stuff.foo.score")));
        assertThat(score.dataType(), is("float"));

        AnalyzedColumnDefinition price = foo.children().get(1);
        assertThat(price.ident(), is(ColumnIdent.fromPath("details.stuff.foo.price")));
        assertThat(price.dataType(), is("string"));

        Map<String, Object> mapping = analysis.analyzedTableElements().toMapping();
        assertThat(mapToSortedString(mapping),
            is("_all={enabled=false}, _meta={}, properties={details={dynamic=true, " +
               "properties={stuff={dynamic=true, properties={foo={dynamic=true, properties={price={type=keyword}, " +
               "score={type=float}}, type=object}}, type=object}}, type=object}}"));
    }

    @Test
    public void testAddGeneratedColumn() throws Exception {
        AddColumnAnalyzedStatement analysis = e.analyze(
            "alter table users add column name_generated as concat(name, 'foo')");

        assertThat(analysis.hasNewGeneratedColumns(), is(true));
        assertThat(analysis.analyzedTableElements().columnIdents(), containsInAnyOrder(
            new ColumnIdent("name_generated"), new ColumnIdent("id")));

        AnalyzedColumnDefinition nameGeneratedColumn = null;
        for (AnalyzedColumnDefinition columnDefinition : analysis.analyzedTableElements().columns()) {
            if (columnDefinition.ident().name().equals("name_generated")) {
                nameGeneratedColumn = columnDefinition;
            }
        }
        assertNotNull(nameGeneratedColumn);
        assertThat(nameGeneratedColumn.dataType(), equalTo("string"));
        assertThat(nameGeneratedColumn.formattedGeneratedExpression(), is("concat(name, 'foo')"));
    }


    @Test
    public void testAddColumnWithColumnStoreDisabled() throws Exception {
        AddColumnAnalyzedStatement analysis = e.analyze(
            "alter table users add column string_no_docvalues string STORAGE WITH (columnstore = false)");
        Map<String, Object> mapping = analysis.analyzedTableElements().toMapping();
        assertThat(mapToSortedString(mapping),
            is("_all={enabled=false}, _meta={primary_keys=[id]}, properties={id={type=long}, " +
               "string_no_docvalues={doc_values=false, type=keyword}}"));
    }
}
