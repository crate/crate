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
import io.crate.testing.MockedClusterServiceModule;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.xcontent.XContentHelper;
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
                new MockedClusterServiceModule(),
                new TestMetaDataModule(),
                new MetaDataSysModule(),
                new PredicateModule()
        ));
        return modules;
    }

    @Test
    public void testInsertWithColumns() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into users (id, name) values (1, 'Trillian')");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("name"));
        assertEquals(DataTypes.STRING, analysis.columns().get(1).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        Map<String, Object> values = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertThat(values.size(), is(2));
        assertThat((Integer) values.get("id"), is(1));
        assertThat((String)values.get("name"), is("Trillian"));
    }

    @Test
    public void testInsertWithTwistedColumns() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into users (name, id) values ('Trillian', 2)");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("name"));
        assertEquals(DataTypes.STRING, analysis.columns().get(0).valueType());

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(1).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        Map<String, Object> values = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertThat(values.size(), is(2));
        assertThat((String)values.get("name"), is("Trillian"));
        assertThat((Integer) values.get("id"), is(2));
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
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement)analyze("insert into users (id, name, awesome) values ($1, 'Trillian', $2)", new Object[]{1.0f, "true"});

        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());
        assertEquals(DataTypes.BOOLEAN, analysis.columns().get(2).valueType());

        Map<String, Object> values = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertThat((Integer) values.get("id"), is(1));
        assertThat((Boolean)values.get("awesome"), is(true));

    }

    @Test
    public void testInsertWithFunction() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into users (id, name) values (ABS(-1), 'Trillian')");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("name"));
        assertEquals(DataTypes.STRING, analysis.columns().get(1).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        Map<String, Object> values = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertThat(values.size(), is(2));
        assertThat((Integer) values.get("id"), is(1)); // normalized/evaluated
        assertThat((String)values.get("name"), is("Trillian"));
    }

    @Test
    public void testInsertWithoutColumns() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into users values (1, 1, 'Trillian')");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(3));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("other_id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(1).valueType());

        assertThat(analysis.columns().get(2).info().ident().columnIdent().name(), is("name"));
        assertEquals(DataTypes.STRING, analysis.columns().get(2).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        Map<String, Object> values = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertThat(values.size(), is(3));
        assertThat((Integer) values.get("id"), is(1));
        assertThat((Integer) values.get("other_id"), is(1));
        assertThat((String)values.get("name"), is("Trillian"));
    }

    @Test
    public void testInsertWithoutColumnsAndOnlyOneColumn() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into users values (1)");
        assertThat(analysis.table().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(1));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        Map<String, Object> values = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertThat(values.size(), is(1));
        assertThat((Integer) values.get("id"), is(1));
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
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement)analyze("insert into users (id, name, awesome, details) values (?, ?, ?, ?)",
                new Object[]{1, null, null, null});
        Map<String, Object> values = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertThat((Integer) values.get("id"), is(1));
        assertNull(values.get("name"));
        assertNull(values.get("awesome"));
        assertNull(values.get("details"));
    }

    @Test
    public void testObjectLiterals() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement)analyze("insert into users (id, name, awesome, details) values (?, ?, ?, ?)",
                new Object[]{1, null, null, new HashMap<String, Object>(){{
                    put("new_col", "new value");
                }}});
        Map<String, Object> values = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
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
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement)analyze("insert into users (id, friends) values(?, ?)",
                new Object[]{ 0, new Map[]{
                        new HashMap<String, Object>() {{ put("name", "Jeltz"); }},
                        new HashMap<String, Object>() {{ put("name", "Prosser"); }}
                    }
                });
        Map<String, Object> values = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertThat((Integer) values.get("id"), is(0));
        assertArrayEquals(
                ((List) values.get("friends")).toArray(),
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

    @Test
    public void testInsertInvalidObjectArrayInObject() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for details: invalid value for object array type");
        analyze("insert into deeply_nested (details) " +
                "values (" +
                "  {awesome=true, arguments=[1,2,3]}" +
                ")");
    }

    @Test
    public void testInsertInvalidObjectArrayFieldInObjectArray() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for tags.metadata.id: Invalid long");
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
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze(
                "insert into deeply_nested (tags) " +
                        "values ([" +
                        "           {\"name\"='cool', \"metadata\"=[{\"id\"=0}, {\"id\"=1}]}, " +
                        "           {\"name\"='fancy', \"metadata\"=[{\"id\"='2'}, {\"id\"=3}]}" +
                        "         ])");
        assertThat(analysis.sourceMaps().size(), is(1));
        Map<String, Object> values = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        List arrayValue = (List)values.get("tags");
        assertThat(arrayValue.size(), is(2));
        assertThat(arrayValue.get(0), instanceOf(Map.class));
        assertThat((String)((Map)arrayValue.get(0)).get("name"), is("cool"));
        assertThat((String)((Map)arrayValue.get(1)).get("name"), is("fancy"));
        assertThat(Arrays.toString(((List)((Map)arrayValue.get(0)).get("metadata")).toArray()), is("[{id=0}, {id=1}]"));
        assertThat(Arrays.toString(((List) ((Map) arrayValue.get(1)).get("metadata")).toArray()), is("[{id=2}, {id=3}]"));
    }

    @Test
    public void testInsertEmptyObjectArrayParameter() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement)analyze("insert into users (id, friends) values(?, ?)",
                new Object[]{ 0, new Map[0] });
        Map<String, Object> values = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertThat((Integer)values.get("id"), is(0));
        assertThat(((List) values.get("friends")).size(), is(0));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testInsertSystemColumn() throws Exception {
        analyze("insert into users (id, _id) values (?, ?)",
                new Object[]{1, "1"});
    }

    @Test
    public void testNestedPk() throws Exception {
        // FYI: insert nested clustered by test here too
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement)analyze("insert into nested_pk (id, o) values (?, ?)",
                new Object[]{1, new MapBuilder<String, Object>().put("b", 4).map()});
        assertThat(analysis.ids().size(), is(1));
        assertThat(analysis.ids().get(0),
                is(new Id(Arrays.asList(new ColumnIdent("id"), new ColumnIdent("o.b")), Arrays.asList(new BytesRef("1"), new BytesRef("4")), new ColumnIdent("o.b")).stringValue()));
    }

    @Test
    public void testNestedPkAllColumns() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement)analyze("insert into nested_pk values (?, ?)",
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
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into nested_pk (o, id) values (?, ?)",
                new Object[]{new MapBuilder<String, Object>().put("b", 4).map(), 1});
        assertThat(analysis.ids().get(0),
                is(new Id(Arrays.asList(new ColumnIdent("id"), new ColumnIdent("o.b")), Arrays.asList(new BytesRef("1"), new BytesRef("4")), new ColumnIdent("o.b")).stringValue()));

    }

    @Test
    public void testInsertMultipleValues() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement)analyze(
                "insert into users (id, name, awesome) values (?, ?, ?), (?, ?, ?)",
                new Object[]{ 99, "Marvin", true, 42, "Deep Thought", false });
        Map<String, Object> values = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertThat(analysis.sourceMaps().size(), is(2));

        assertThat((Integer) values.get("id"), is(99));
        assertThat((String) values.get("name"), is("Marvin"));
        assertThat((Boolean) values.get("awesome"), is(true));

        values = XContentHelper.convertToMap(analysis.sourceMaps().get(1), false).v2();
        assertThat((Integer) values.get("id"), is(42));
        assertThat((String) values.get("name"), is("Deep Thought"));
        assertThat((Boolean) values.get("awesome"), is(false));
    }

    @Test
    public void testInsertPartitionedTable() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into parted (id, name, date) " +
                "values (?, ?, ?)", new Object[]{0, "Trillian", 0L});
        Map<String, Object> values = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(values.size(), is(2));
        assertThat(analysis.columns().size(), is(3));
        assertThat(analysis.partitionMaps().size(), is(1));
        assertThat(analysis.partitionMaps().get(0), hasEntry("date", "0"));
    }

    @Test
    public void testInsertIntoPartitionedTableOnlyPartitionColumns() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into parted (date) " +
                "values (?)", new Object[]{0L});
        Map<String, Object> values = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(values.size(), is(0));
        assertThat(analysis.columns().size(), is(1));
        assertThat(analysis.partitionMaps().size(), is(1));
        assertThat(analysis.partitionMaps().get(0), hasEntry("date", "0"));
    }

    @Test
    public void bulkIndexPartitionedTable() throws Exception {
        // multiple values
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into parted (id, name, date) " +
                        "values (?, ?, ?), (?, ?, ?), (?, ?, ?)",
                new Object[]{
                        1, "Trillian", 13963670051500L,
                        2, "Ford", 0L,
                        3, "Zaphod", null
                });
        validateBulkIndexPartitionedTableAnalysis(analysis);
        // bulk args
        analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into parted (id, name, date) " +
                        "values (?, ?, ?)",
                new Object[][]{
                        new Object[]{ 1, "Trillian", 13963670051500L },
                        new Object[]{ 2, "Ford", 0L },
                        new Object[]{ 3, "Zaphod", null}
                });
        validateBulkIndexPartitionedTableAnalysis(analysis);
    }

    private void validateBulkIndexPartitionedTableAnalysis(InsertFromValuesAnalyzedStatement analysis) {
        assertThat(analysis.partitions(), contains(
                new PartitionName("parted", Arrays.asList(new BytesRef("13963670051500"))).stringValue(),
                new PartitionName("parted", Arrays.asList(new BytesRef("0"))).stringValue(),
                new PartitionName("parted", new ArrayList<BytesRef>() {{
                    add(null);
                }}).stringValue()
        ));
        assertThat(analysis.sourceMaps().size(), is(3));
        Map<String, Object> values = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertThat(values, allOf(hasEntry("name", (Object) "Trillian"), hasEntry("id", (Object) 1)));
        values = XContentHelper.convertToMap(analysis.sourceMaps().get(1), false).v2();
        assertThat(values, allOf(hasEntry("name", (Object) "Ford"), hasEntry("id", (Object) 2)));
        values = XContentHelper.convertToMap(analysis.sourceMaps().get(2), false).v2();
        assertThat(values, allOf(hasEntry("name", (Object) "Zaphod"), hasEntry("id", (Object) 3)));

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
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into nested_parted (id, date, obj)" +
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
        InsertFromValuesAnalyzedStatement analysis;
        analysis = (InsertFromValuesAnalyzedStatement) analyze(
                "insert into users (id, name) values (?, ?)",
                new Object[][]{
                        new Object[]{1, "foo"},
                        new Object[]{2, "bar"}
                });
        assertThat(analysis.sourceMaps().size(), is(2));
        Map<String, Object> args1 = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertThat((Integer) args1.get("id"), is(1));

        Map<String, Object> args2 = XContentHelper.convertToMap(analysis.sourceMaps().get(1), false).v2();
        assertThat((Integer) args2.get("id"), is(2));
    }

    @Test
    public void testInsertWithBulkArgsMultiValue() throws Exception {
        // should be equal to testInsertWithBulkArgs()
        InsertFromValuesAnalyzedStatement analysis;
        analysis = (InsertFromValuesAnalyzedStatement) analyze(
                "insert into users (id, name) values (?, ?), (?, ?)",
                new Object[][]{
                        {1, "foo", 2, "bar"}  // one bulk row
                });
        assertThat(analysis.sourceMaps().size(), is(2));
        Map<String, Object> args1 = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertThat((Integer) args1.get("id"), is(1));

        Map<String, Object> args2 = XContentHelper.convertToMap(analysis.sourceMaps().get(1), false).v2();
        assertThat((Integer) args2.get("id"), is(2));
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
    public void testInsertWithBulkArgsNullValues() throws Exception {
        InsertFromValuesAnalyzedStatement analysis;
        analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into users (id, name) values (?, ?)",
                new Object[][] {
                        new Object[] { 10, "foo" },
                        new Object[] { 11, null }
                });

        assertThat(analysis.sourceMaps().size(), is(2));
        Map<String, Object> args1 = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertEquals(args1.get("name"), "foo");
        Map<String, Object> args2 = XContentHelper.convertToMap(analysis.sourceMaps().get(1), false).v2();
        assertEquals(args2.get("name"), null);
    }

    @Test
    public void testInsertWithBulkArgsNullValuesFirst() throws Exception {
        InsertFromValuesAnalyzedStatement analysis;
        analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into users (id, name) values (?, ?)",
                new Object[][] {
                        new Object[] { 12, null },
                        new Object[] { 13, "foo" },
                });

        assertThat(analysis.sourceMaps().size(), is(2));
        Map<String, Object> args1 = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertEquals(args1.get("name"), null);
        Map<String, Object> args2 = XContentHelper.convertToMap(analysis.sourceMaps().get(1), false).v2();
        assertEquals(args2.get("name"), "foo");
    }

    @Test
    public void testInsertWithBulkArgsArrayNullValuesFirst() throws Exception {
        InsertFromValuesAnalyzedStatement analysis;
        analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into users (id, new_col) values (?, ?)",
                        new Object[] { 12, new String[]{ null }  });
        analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into users (id, new_col) values (?, ?)",
                new Object[][] {
                        new Object[] { 12, new String[]{ null }  },
                        new Object[] { 13, new String[]{ "foo" } },
                });

        assertThat(analysis.sourceMaps().size(), is(2));
        Map<String, Object> args1 = XContentHelper.convertToMap(analysis.sourceMaps().get(0), false).v2();
        assertEquals(args1.get("new_col"), new ArrayList<String>(){{ add(null); }});
        Map<String, Object> args2 = XContentHelper.convertToMap(analysis.sourceMaps().get(1), false).v2();
        assertEquals(args2.get("new_col"), new ArrayList<String>(){{ add("foo"); }});
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


    @Test
    public void testInsertWithTooFewArguments() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Tried to resolve a parameter but the arguments provided with the SQLRequest don't contain a parameter at position 1");
        analyze("insert into users (id, name) values (?, ?)", new Object[] { 1 });
    }

    @Test
    public void testInsertWithTooFewBulkArguments() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Tried to resolve a parameter but the arguments provided with the SQLRequest don't contain a parameter at position 0");
        analyze("insert into users (id, name) values (?, ?)", new Object[][]{
                new Object[]{},
                new Object[]{}
        });
    }

    @Test
    public void testRejectNestedArrayLiteral() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for tags: Invalid datatype 'string_array_array'");

        analyze("insert into users (id, name, tags) values (42, 'Deep Thought', [['the', 'answer'], ['what''s', 'the', 'question', '?']])");

    }

    @Test
    public void testRejectNestedArrayParam() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for tags: Invalid datatype 'string_array_array'");

        analyze("insert into users (id, name, tags) values (42, 'Deep Thought', ?)", new Object[]{
                new String[][] {
                        new String[] {"the", "answer"},
                        new String[] {"what's", "the", "question", "?"}
                }
        });

    }

    @Test
    public void testRejectNestedArrayBulkParam() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for tags: Invalid datatype 'string_array_array'");

        analyze("insert into users (id, name, tags) values (42, 'Deep Thought', ?)", new Object[][]{
                new Object[] {
                        new String[][] {
                                new String[] {"the", "answer"},
                                new String[] {"what's", "the", "question", "?"}
                        }
                }
        });
    }

    @Test
    public void testRejectDynamicNestedArrayLiteral() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for theses: Invalid datatype 'string_array_array'");

        analyze("insert into users (id, name, theses) values (1, 'Marx', [[" +
                "'Feuerbach, mit dem abstrakten Denken nicht zufrieden, appellirt an die sinnliche Anschauung', " +
                "'aber er fasst die Sinnlichkeit nicht als praktische, menschlich-sinnliche Thätigkeit']])");
    }

    @Test
    public void testRejectDynamicNestedArrayParam() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for theses: Invalid datatype 'string_array_array'");

        analyze("insert into users (id, name, theses) values (1, 'Marx', ?)", new Object[]{
                new String[][]{
                        new String[]{ "Feuerbach, mit dem abstrakten Denken nicht zufrieden, appellirt an die sinnliche Anschauung" },
                        new String[]{ "aber er fasst die Sinnlichkeit nicht als praktische, menschlich-sinnliche Thätigkeit" }
                }
        });
    }

    @Test
    public void testRejectDynamicNestedArrayBulkParam() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for theses: Invalid datatype 'string_array_array'");

        analyze("insert into users (id, name, theses) values (1, 'Marx', ?)", new Object[][]{
                new Object[] {
                        new String[][]{
                                new String[]{ "Feuerbach, mit dem abstrakten Denken nicht zufrieden, appellirt an die sinnliche Anschauung" },
                                new String[]{ "aber er fasst die Sinnlichkeit nicht als praktische, menschlich-sinnliche Thätigkeit" }
                        }
                }
        });
    }
}

