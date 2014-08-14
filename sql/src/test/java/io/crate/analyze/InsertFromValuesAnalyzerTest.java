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

package io.crate.analyze;

import io.crate.PartitionName;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.predicate.PredicateModule;
import io.crate.planner.RowGranularity;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Module;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InsertFromValuesAnalyzerTest extends BaseAnalyzerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final TableIdent TEST_ALIAS_TABLE_IDENT = new TableIdent(null, "alias");
    private static final TableInfo TEST_ALIAS_TABLE_INFO = new TestingTableInfo.Builder(
            TEST_ALIAS_TABLE_IDENT, RowGranularity.DOC, new Routing())
            .add("bla", DataTypes.STRING, null)
            .isAlias(true).build();

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(TEST_DOC_TABLE_IDENT.name())).thenReturn(userTableInfo);
            when(schemaInfo.getTableInfo(TEST_ALIAS_TABLE_IDENT.name())).thenReturn(TEST_ALIAS_TABLE_INFO);
            when(schemaInfo.getTableInfo(NESTED_PK_TABLE_IDENT.name())).thenReturn(nestedPkTableInfo);
            when(schemaInfo.getTableInfo(TEST_PARTITIONED_TABLE_IDENT.name()))
                    .thenReturn(TEST_PARTITIONED_TABLE_INFO);
            when(schemaInfo.getTableInfo(TEST_NESTED_PARTITIONED_TABLE_IDENT.name()))
                    .thenReturn(TEST_NESTED_PARTITIONED_TABLE_INFO);
            when(schemaInfo.getTableInfo(DEEPLY_NESTED_TABLE_IDENT.name()))
                    .thenReturn(DEEPLY_NESTED_TABLE_INFO);
            schemaBinder.addBinding(DocSchemaInfo.NAME).toInstance(schemaInfo);
        }

        @Override
        protected void bindFunctions() {
            super.bindFunctions();
            functionBinder.addBinding(ABS_FUNCTION_INFO.ident()).to(AbsFunction.class);
        }
    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                new TestModule(),
                new TestMetaDataModule(),
                new MetaDataSysModule(),
                new PredicateModule()
        ));
        return modules;
    }

    @Test
    public void testInsertWithColumns() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis) analyze("insert into users (id, name) values (1, 'Trillian')");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("name"));
        assertEquals(DataTypes.STRING, analysis.columns().get(1).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        Map<String, Object> values = analysis.sourceMaps().get(0);
        assertThat(values.size(), is(2));
        assertThat((Long)values.get("id"), is(1L));
        assertThat((String)values.get("name"), is("Trillian"));
    }

    @Test
    public void testInsertWithTwistedColumns() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis) analyze("insert into users (name, id) values ('Trillian', 2)");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("name"));
        assertEquals(DataTypes.STRING, analysis.columns().get(0).valueType());

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(1).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        Map<String, Object> values = analysis.sourceMaps().get(0);
        assertThat(values.size(), is(2));
        assertThat((String)values.get("name"), is("Trillian"));
        assertThat((Long)values.get("id"), is(2L));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInsertWithColumnsAndTooManyValues() throws Exception {
        analyze("insert into users (name, id) values ('Trillian', 2, true)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInsertWithColumnsAndTooLessValues() throws Exception {
        analyze("insert into users (name, id) values ('Trillian')");
    }

    @Test(expected = ValidationException.class)
    public void testInsertWithWrongType() throws Exception {
        analyze("insert into users (name, id) values (1, 'Trillian')");
    }

    @Test
    public void testInsertWithNumericTypeOutOfRange() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for bytes: byte value out of range: 1234");

        analyze("insert into users (name, id, bytes) values ('Trillian', 4, 1234)");
    }


    @Test(expected = ValidationException.class)
    public void testInsertWithWrongParameterType() throws Exception {
        analyze("insert into users (name, id) values (?, ?)", new Object[]{1, true});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInsertSameReferenceRepeated() throws Exception {
        analyze("insert into users (name, name) values ('Trillian', 'Ford')");
    }

    @Test
    public void testInsertWithConvertedTypes() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis)analyze("insert into users (id, name, awesome) values ($1, 'Trillian', $2)", new Object[]{1.0f, "true"});

        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());
        assertEquals(DataTypes.BOOLEAN, analysis.columns().get(2).valueType());

        Map<String, Object> values = analysis.sourceMaps().get(0);
        assertThat((Long) values.get("id"), is(1L));
        assertThat((Boolean)values.get("awesome"), is(true));

    }

    @Test
    public void testInsertWithFunction() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis) analyze("insert into users (id, name) values (ABS(-1), 'Trillian')");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("name"));
        assertEquals(DataTypes.STRING, analysis.columns().get(1).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        Map<String, Object> values = analysis.sourceMaps().get(0);
        assertThat(values.size(), is(2));
        assertThat((Long)values.get("id"), is(1L)); // normalized/evaluated
        assertThat((String)values.get("name"), is("Trillian"));
    }

    @Test
    public void testInsertWithoutColumns() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis) analyze("insert into users values (1, 1, 'Trillian')");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(3));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("other_id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(1).valueType());

        assertThat(analysis.columns().get(2).info().ident().columnIdent().name(), is("name"));
        assertEquals(DataTypes.STRING, analysis.columns().get(2).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        Map<String, Object> values = analysis.sourceMaps().get(0);
        assertThat(values.size(), is(3));
        assertThat((Long)values.get("id"), is(1L));
        assertThat((Long)values.get("other_id"), is(1L));
        assertThat((String)values.get("name"), is("Trillian"));
    }

    @Test
    public void testInsertWithoutColumnsAndOnlyOneColumn() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis) analyze("insert into users values (1)");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(1));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        Map<String, Object> values = analysis.sourceMaps().get(0);
        assertThat(values.size(), is(1));
        assertThat((Long) values.get("id"), is(1L));
    }

    @Test (expected = UnsupportedOperationException.class)
    public void testInsertIntoSysTable() throws Exception {
        analyze("insert into sys.nodes (id, name) values (666, 'evilNode')");
    }

    @Test (expected = UnsupportedOperationException.class)
    public void testInsertIntoAliasTable() throws Exception {
        analyze("insert into alias (bla) values ('blubb')");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testInsertWithoutPrimaryKey() throws Exception {
        analyze("insert into users (name) values ('Trillian')");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testNullPrimaryKey() throws Exception {
        analyze("insert into users (id) values (?)",
                new Object[]{null});
    }

    @Test
    public void testNullLiterals() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis)analyze("insert into users (id, name, awesome, details) values (?, ?, ?, ?)",
                new Object[]{1, null, null, null});
        Map<String, Object> values = analysis.sourceMaps().get(0);
        assertThat((Long) values.get("id"), is(1L));
        assertNull(values.get("name"));
        assertNull(values.get("awesome"));
        assertNull(values.get("details"));
    }

    @Test
    public void testObjectLiterals() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis)analyze("insert into users (id, name, awesome, details) values (?, ?, ?, ?)",
                new Object[]{1, null, null, new HashMap<String, Object>(){{
                    put("new_col", "new value");
                }}});
        Map<String, Object> values = analysis.sourceMaps().get(0);
        assertThat(values.get("details"), instanceOf(Map.class));
    }

    @Test(expected = ColumnValidationException.class)
    public void testInsertArrays() throws Exception {
        // error because in the schema are non-array types:
        analyze("insert into users (id, name, awesome, details) values (?, ?, ?, ?)",
                new Object[]{
                        new Long[]{1l, 2l},
                        new String[]{"Karl Liebknecht", "Rosa Luxemburg"},
                        new Boolean[]{true, false},
                        new Map[]{
                                new HashMap<String, Object>(),
                                new HashMap<String, Object>()
                        }
                }
        );
    }

    @Test
    public void testInsertObjectArrayParameter() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis)analyze("insert into users (id, friends) values(?, ?)",
                new Object[]{ 0, new Map[]{
                        new HashMap<String, Object>() {{ put("name", "Jeltz"); }},
                        new HashMap<String, Object>() {{ put("name", "Prosser"); }}
                    }
                });
        assertThat((Long)analysis.sourceMaps().get(0).get("id"), is(0L));
        assertArrayEquals(
                (Object[]) analysis.sourceMaps().get(0).get("friends"),
                new Object[]{
                        new MapBuilder<String, Object>().put("name", "Jeltz").map(),
                        new MapBuilder<String, Object>().put("name", "Prosser").map(),
                }
        );
    }

    @Test(expected = ColumnValidationException.class)
    public void testInsertInvalidObjectArrayParameter1() throws Exception {
        analyze("insert into users (id, friends) values(?, ?)",
                new Object[]{0, new Map[]{
                        new HashMap<String, Object>() {{
                            put("id", "Jeltz");
                        }}
                }
                });
    }

    @Test(expected = ColumnValidationException.class)
    public void testInsertInvalidObjectArrayParameter2() throws Exception {
        analyze("insert into users (id, friends) values(?, ?)",
                new Object[]{0, new Map[]{
                        new HashMap<String, Object>() {{
                            put("id", 1L);
                            put("groups", "a");
                        }}
                }
                });
    }

    @Test(expected = ColumnValidationException.class)
    public void testInsertInvalidObjectArrayInObject() throws Exception {
        analyze("insert into deeply_nested (details) " +
                "values (" +
                "  {awesome=true, arguments=[1,2,3]}" +
                ")");
    }

    @Test(expected = ColumnValidationException.class)
    public void testInsertInvalidObjectArrayFieldInObjectArray() throws Exception {
        analyze("insert into deeply_nested (tags) " +
                "values (" +
                "  [" +
                "    {name='right', metadata=[{id=1}, {id=2}]}," +
                "    {name='wrong', metadata=[{id='foo'}]}" +
                "  ]" +
                ")");
    }

    @Test
    public void testInsertNestedObjectLiteral() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis) analyze(
                "insert into deeply_nested (tags) " +
                        "values ([" +
                        "           {\"name\"='cool', \"metadata\"=[{\"id\"=0}, {\"id\"=1}]}, " +
                        "           {\"name\"='fancy', \"metadata\"=[{\"id\"='2'}, {\"id\"=3}]}" +
                        "         ])");
        assertThat(analysis.sourceMaps().size(), is(1));
        Object[] arrayValue = (Object[])analysis.sourceMaps().get(0).get("tags");
        assertThat(arrayValue.length, is(2));
        assertThat(arrayValue[0], instanceOf(Map.class));
        assertThat((String)((Map<String, Object>)arrayValue[0]).get("name"), is("cool"));
        assertThat((String)((Map<String, Object>)arrayValue[1]).get("name"), is("fancy"));
        assertThat(Arrays.toString((Object[])((Map<String, Object>)arrayValue[0]).get("metadata")), is("[{id=0}, {id=1}]"));
        assertThat(Arrays.toString((Object[]) ((Map<String, Object>) arrayValue[1]).get("metadata")), is("[{id=2}, {id=3}]"));
    }

    @Test
    public void testInsertEmptyObjectArrayParameter() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis)analyze("insert into users (id, friends) values(?, ?)",
                new Object[]{ 0, new Map[0] });
        assertThat((Long)analysis.sourceMaps().get(0).get("id"), is(0L));
        assertThat(((Object[]) analysis.sourceMaps().get(0).get("friends")).length, is(0));

    }

    @Test (expected = IllegalArgumentException.class)
    public void testInsertSystemColumn() throws Exception {
        analyze("insert into users (id, _id) values (?, ?)",
                new Object[]{1, "1"});
    }

    @Test
    public void testNestedPk() throws Exception {
        // FYI: insert nested clustered by test here too
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis)analyze("insert into nested_pk (id, o) values (?, ?)",
                new Object[]{1, new MapBuilder<String, Object>().put("b", 4).map()});
        assertThat(analysis.ids().size(), is(1));
        assertThat(analysis.ids().get(0),
                is(new Id(Arrays.asList(new ColumnIdent("id"), new ColumnIdent("o.b")), Arrays.asList(new BytesRef("1"), new BytesRef("4")), new ColumnIdent("o.b")).stringValue()));
    }

    @Test
    public void testNestedPkAllColumns() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis)analyze("insert into nested_pk values (?, ?)",
                new Object[]{1, new MapBuilder<String, Object>().put("b", 4).map()});
        assertThat(analysis.ids().size(), is(1));
        assertThat(analysis.ids().get(0),
                is(new Id(Arrays.asList(new ColumnIdent("id"), new ColumnIdent("o.b")), Arrays.asList(new BytesRef("1"), new BytesRef("4")), new ColumnIdent("o.b")).stringValue()));
    }

    @Test( expected = IllegalArgumentException.class)
    public void testMissingNestedPk() throws Exception {
        analyze("insert into nested_pk (id) values (?)", new Object[]{1});
    }

    @Test( expected = IllegalArgumentException.class)
    public void testMissingNestedPkInMap() throws Exception {
        analyze("insert into nested_pk (id, o) values (?, ?)", new Object[]{1, new HashMap<String, Object>()});
    }

    @Test
    public void testTwistedNestedPk() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis) analyze("insert into nested_pk (o, id) values (?, ?)",
                new Object[]{new MapBuilder<String, Object>().put("b", 4).map(), 1});
        assertThat(analysis.ids().get(0),
                is(new Id(Arrays.asList(new ColumnIdent("id"), new ColumnIdent("o.b")), Arrays.asList(new BytesRef("1"), new BytesRef("4")), new ColumnIdent("o.b")).stringValue()));

    }

    @Test
    public void testInsertMultipleValues() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis)analyze(
                "insert into users (id, name, awesome) values (?, ?, ?), (?, ?, ?)",
                new Object[]{ 99, "Marvin", true, 42, "Deep Thought", false });
        assertThat(analysis.sourceMaps().size(), is(2));

        assertThat((Long)analysis.sourceMaps().get(0).get("id"), is(99L));
        assertThat((String)analysis.sourceMaps().get(0).get("name"), is("Marvin"));
        assertThat((Boolean)analysis.sourceMaps().get(0).get("awesome"), is(true));

        assertThat((Long)analysis.sourceMaps().get(1).get("id"), is(42l));
        assertThat((String)analysis.sourceMaps().get(1).get("name"), is("Deep Thought"));
        assertThat((Boolean)analysis.sourceMaps().get(1).get("awesome"), is(false));
    }

    @Test
    public void testInsertPartitionedTable() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis) analyze("insert into parted (id, name, date) " +
                "values (?, ?, ?)", new Object[]{0, "Trillian", 0L});
        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(analysis.sourceMaps().get(0).size(), is(2));
        assertThat(analysis.columns().size(), is(3));
        assertThat(analysis.partitionMaps().size(), is(1));
        assertThat(analysis.partitionMaps().get(0), hasEntry("date", "0"));
    }

    @Test
    public void testInsertIntoPartitionedTableOnlyPartitionColumns() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis) analyze("insert into parted (date) " +
                "values (?)", new Object[]{0L});
        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(analysis.sourceMaps().get(0).size(), is(0));
        assertThat(analysis.columns().size(), is(1));
        assertThat(analysis.partitionMaps().size(), is(1));
        assertThat(analysis.partitionMaps().get(0), hasEntry("date", "0"));
    }

    @Test
    public void bulkIndexPartitionedTable() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis) analyze("insert into parted (id, name, date) " +
                "values (?, ?, ?), (?, ?, ?), (?, ?, ?)",
                new Object[]{
                        1, "Trillian", 13963670051500L,
                        2, "Ford", 0L,
                        3, "Zaphod", null
                });
        assertThat(analysis.partitions(), contains(
                new PartitionName("parted", Arrays.asList(new BytesRef("13963670051500"))).stringValue(),
                new PartitionName("parted", Arrays.asList(new BytesRef("0"))).stringValue(),
                new PartitionName("parted", new ArrayList<BytesRef>() {{
                    add(null);
                }}).stringValue()
        ));
        assertThat(analysis.sourceMaps().size(), is(3));
        assertThat(analysis.sourceMaps().get(0),
                allOf(hasEntry("name", (Object) "Trillian"), hasEntry("id", (Object) 1)));
        assertThat(analysis.sourceMaps().get(1),
                allOf(hasEntry("name", (Object) "Ford"), hasEntry("id", (Object) 2)));
        assertThat(analysis.sourceMaps().get(2),
                allOf(hasEntry("name", (Object) "Zaphod"), hasEntry("id", (Object) 3)));

        assertThat(analysis.partitionMaps().size(), is(3));
        assertThat(analysis.partitionMaps().get(0),
                hasEntry("date", "13963670051500"));
        assertThat(analysis.partitionMaps().get(1),
                hasEntry("date", "0"));
        assertThat(analysis.partitionMaps().get(2),
                hasEntry("date", null));

    }

    @Test
    public void testInsertWithMatchPredicateInValues() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Invalid value of type 'FUNCTION'");
        analyze("insert into users (id, awesome) values (1, match(name, 'bar'))");
    }

    @Test
    public void testInsertNestedPartitionedColumn() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis) analyze("insert into nested_parted (id, date, obj)" +
                "values (?, ?, ?), (?, ?, ?)",
                new Object[]{
                        1, "1970-01-01", new MapBuilder<String, Object>().put("name", "Zaphod").map(),
                        2, "2014-05-21", new MapBuilder<String, Object>().put("name", "Arthur").map()
                });
        assertThat(analysis.partitions(), contains(
                new PartitionName("nested_parted", Arrays.asList(new BytesRef("0"), new BytesRef("Zaphod"))).stringValue(),
                new PartitionName("nested_parted", Arrays.asList(new BytesRef("1400630400000"), new BytesRef("Arthur"))).stringValue()

        ));
        assertThat(analysis.sourceMaps().size(), is(2));
    }

    @Test
    public void testInsertWithBulkArgs() throws Exception {
        InsertFromValuesAnalysis analysis = (InsertFromValuesAnalysis) analyze(
                "insert into users (id, name) values (?, ?)",
                new Object[][]{
                        new Object[]{1, "foo"},
                        new Object[]{2, "bar"}
        });
        assertThat(analysis.sourceMaps().size(), is(2));

        Map<String, Object> args1 = analysis.sourceMaps().get(0);
        assertThat((Long) args1.get("id"), is(1L));

        Map<String, Object> args2 = analysis.sourceMaps().get(1);
        assertThat((Long) args2.get("id"), is(2L));
    }

    @Test
    public void testInsertWithBulkArgsTypeMissMatch() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("argument 1 of bulk arguments contains mixed data types");
        analyze("insert into users (id, name) values (?, ?)",
                new Object[][]{
                        new Object[]{10, "foo"},
                        new Object[]{"11", "bar"}
                }
        );
    }

    @Test
    public void testInsertWithBulkArgsMixedLength() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("mixed number of arguments inside bulk arguments");
        analyze("insert into users (id, name) values (?, ?)",
                new Object[][]{
                        new Object[]{10, "foo"},
                        new Object[]{"11"}
                }
        );
    }

    @Test
    public void testInsertBulkArgWithFirstArgsContainsUnrecognizableObject() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(Matchers.allOf(
                Matchers.startsWith("Got an argument \""),
                Matchers.endsWith("that couldn't be recognized")
        ));

        analyze("insert into users (id, name) values (?, ?)", new Object[][]{
                new Object[]{new Foo()},
        });
    }

    @Test
    public void testInsertBulkArgWithUnrecognizableObject() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(Matchers.allOf(
                Matchers.startsWith("Got an argument \""),
                Matchers.endsWith("that couldn't be recognized")
        ));

        analyze("insert into users (id, name) values (?, ?)", new Object[][]{
                new Object[]{"foo"},
                new Object[]{new Foo()},
        });
    }

    private static class Foo {}
}
