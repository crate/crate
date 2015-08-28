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

import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.InvalidColumnNameException;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.*;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.operation.scalar.arithmetic.AddFunction;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Module;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.*;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InsertFromValuesAnalyzerTest extends BaseAnalyzerTest {

    private static final TableIdent TEST_ALIAS_TABLE_IDENT = new TableIdent(null, "alias");
    private static final TableInfo TEST_ALIAS_TABLE_INFO = new TestingTableInfo.Builder(
            TEST_ALIAS_TABLE_IDENT, new Routing())
            .add("bla", DataTypes.STRING, null)
            .isAlias(true).build();

    private static final TableIdent NESTED_CLUSTERED_TABLE_IDENT = new TableIdent(null, "nested_clustered");
    private static final TableInfo NESTED_CLUSTERED_TABLE_INFO = new TestingTableInfo.Builder(
            NESTED_CLUSTERED_TABLE_IDENT, new Routing())
            .add("o", DataTypes.OBJECT, null)
            .add("o", DataTypes.STRING, Arrays.asList("c"))
            .clusteredBy("o.c")
            .build();

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
            when(schemaInfo.getTableInfo(NESTED_CLUSTERED_TABLE_IDENT.name()))
                    .thenReturn(NESTED_CLUSTERED_TABLE_INFO);
            schemaBinder.addBinding(Schemas.DEFAULT_SCHEMA_NAME).toInstance(schemaInfo);
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
                new ScalarFunctionModule(),
                new PredicateModule()
        ));
        return modules;
    }

    @Test
    public void testInsertWithColumns() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into users (id, name) values (1, 'Trillian')");
        assertThat(analysis.tableInfo().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("name"));
        assertEquals(DataTypes.STRING, analysis.columns().get(1).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(analysis.sourceMaps().get(0).length, is(2));
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1L));
        assertThat((BytesRef) analysis.sourceMaps().get(0)[1], is(new BytesRef("Trillian")));
    }

    @Test
    public void testInsertWithTwistedColumns() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into users (name, id) values ('Trillian', 2)");
        assertThat(analysis.tableInfo().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("name"));
        assertEquals(DataTypes.STRING, analysis.columns().get(0).valueType());

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(1).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(analysis.sourceMaps().get(0).length, is(2));
        assertThat((BytesRef) analysis.sourceMaps().get(0)[0], is(new BytesRef("Trillian")));
        assertThat((Long) analysis.sourceMaps().get(0)[1], is(2L));
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

        assertThat(analysis.sourceMaps().get(0).length, is(3));
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1L));
        assertThat((Boolean) analysis.sourceMaps().get(0)[2], is(true));
    }

    @Test
    public void testInsertWithFunction() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into users (id, name) values (ABS(-1), 'Trillian')");
        assertThat(analysis.tableInfo().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("name"));
        assertEquals(DataTypes.STRING, analysis.columns().get(1).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(analysis.sourceMaps().get(0).length, is(2));
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1L));
        assertThat((BytesRef) analysis.sourceMaps().get(0)[1], is(new BytesRef("Trillian")));
    }

    @Test
    public void testInsertWithoutColumns() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into users values (1, 1, 'Trillian')");
        assertThat(analysis.tableInfo().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(3));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());

        assertThat(analysis.columns().get(1).info().ident().columnIdent().name(), is("other_id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(1).valueType());

        assertThat(analysis.columns().get(2).info().ident().columnIdent().name(), is("name"));
        assertEquals(DataTypes.STRING, analysis.columns().get(2).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(analysis.sourceMaps().get(0).length, is(3));
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1L));
        assertThat((Long) analysis.sourceMaps().get(0)[1], is(1L));
        assertThat((BytesRef) analysis.sourceMaps().get(0)[2], is(new BytesRef("Trillian")));
    }

    @Test
    public void testInsertWithoutColumnsAndOnlyOneColumn() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into users values (1)");
        assertThat(analysis.tableInfo().ident(), is(TEST_DOC_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(1));

        assertThat(analysis.columns().get(0).info().ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(analysis.sourceMaps().get(0).length, is(1));
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1L));
    }

    @Test (expected = UnsupportedOperationException.class)
    public void testInsertIntoSysTable() throws Exception {
        analyze("insert into sys.nodes (id, name) values (666, 'evilNode')");
    }

    @Test
    public void testInsertIntoAliasTable() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("alias is an alias. Write, Drop or Alter operations are not supported");
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
        assertThat(analysis.sourceMaps().get(0).length, is(4));
        assertNull(analysis.sourceMaps().get(0)[1]);
        assertNull(analysis.sourceMaps().get(0)[2]);
        assertNull(analysis.sourceMaps().get(0)[3]);
    }

    @Test
    public void testObjectLiterals() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement)analyze("insert into users (id, name, awesome, details) values (?, ?, ?, ?)",
                new Object[]{1, null, null, new HashMap<String, Object>(){{
                    put("new_col", "new value");
                }}});
        assertThat(analysis.sourceMaps().get(0).length, is(4));
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1L));
        assertThat(analysis.sourceMaps().get(0)[3], instanceOf(Map.class));
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
        assertThat(analysis.sourceMaps().get(0).length, is(2));
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(0L));
        assertArrayEquals(
                (Object[]) analysis.sourceMaps().get(0)[1],
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
        expectedException.expectMessage("Validation failed for tags['metadata']['id']: Invalid long");
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
        Object[] arrayValue = (Object[])analysis.sourceMaps().get(0)[0];
        assertThat(arrayValue.length, is(2));
        assertThat(arrayValue[0], instanceOf(Map.class));
        assertThat((String)((Map)arrayValue[0]).get("name"), is("cool"));
        assertThat((String) ((Map) arrayValue[1]).get("name"), is("fancy"));
        assertThat(Arrays.toString(((Object[]) ((Map) arrayValue[0]).get("metadata"))), is("[{id=0}, {id=1}]"));
        assertThat(Arrays.toString(((Object[]) ((Map) arrayValue[1]).get("metadata"))), is("[{id=2}, {id=3}]"));
    }

    @Test
    public void testInsertEmptyObjectArrayParameter() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement)analyze("insert into users (id, friends) values(?, ?)",
                new Object[]{ 0, new Map[0] });
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(0L));
        assertThat(((Object[]) analysis.sourceMaps().get(0)[1]).length, is(0));
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
                is(generateId(Arrays.asList(new ColumnIdent("id"), new ColumnIdent("o.b")), Arrays.asList(new BytesRef("1"), new BytesRef("4")), new ColumnIdent("o.b"))));
    }

    @Test
    public void testNestedPkAllColumns() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement)analyze("insert into nested_pk values (?, ?)",
                new Object[]{1, new MapBuilder<String, Object>().put("b", 4).map()});
        assertThat(analysis.ids().size(), is(1));
        assertThat(analysis.ids().get(0),
                is(generateId(Arrays.asList(new ColumnIdent("id"), new ColumnIdent("o.b")), Arrays.asList(new BytesRef("1"), new BytesRef("4")), new ColumnIdent("o.b"))));
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
                is(generateId(Arrays.asList(new ColumnIdent("id"), new ColumnIdent("o.b")), Arrays.asList(new BytesRef("1"), new BytesRef("4")), new ColumnIdent("o.b"))));

    }

    private String generateId(List<ColumnIdent> pkColumns, List<BytesRef> pkValues, ColumnIdent clusteredBy) {
        return Id.compile(pkColumns, clusteredBy).apply(pkValues);
    }

    @Test
    public void testInsertMultipleValues() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement)analyze(
                "insert into users (id, name, awesome) values (?, ?, ?), (?, ?, ?)",
                new Object[]{ 99, "Marvin", true, 42, "Deep Thought", false });
        assertThat(analysis.sourceMaps().size(), is(2));

        assertThat((Long) analysis.sourceMaps().get(0)[0], is(99L));
        assertThat((BytesRef) analysis.sourceMaps().get(0)[1], is(new BytesRef("Marvin")));
        assertThat((Boolean) analysis.sourceMaps().get(0)[2], is(true));

        assertThat((Long) analysis.sourceMaps().get(1)[0], is(42L));
        assertThat((BytesRef) analysis.sourceMaps().get(1)[1], is(new BytesRef("Deep Thought")));
        assertThat((Boolean) analysis.sourceMaps().get(1)[2], is(false));
    }

    @Test
    public void testInsertPartitionedTable() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into parted (id, name, date) " +
                "values (?, ?, ?)", new Object[]{0, "Trillian", 0L});
        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(analysis.sourceMaps().get(0).length, is(3));
        assertThat(analysis.columns().size(), is(3));
        assertThat(analysis.partitionMaps().size(), is(1));
        assertThat(analysis.partitionMaps().get(0), hasEntry("date", "0"));
    }

    @Test
    public void testInsertIntoPartitionedTableOnlyPartitionColumns() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into parted (date) " +
                "values (?)", new Object[]{0L});
        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(analysis.sourceMaps().get(0).length, is(1));
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
        assertThat(analysis.generatePartitions(), contains(
                new PartitionName("parted", Arrays.asList(new BytesRef("13963670051500"))).stringValue(),
                new PartitionName("parted", Arrays.asList(new BytesRef("0"))).stringValue(),
                new PartitionName("parted", new ArrayList<BytesRef>() {{
                    add(null);
                }}).stringValue()
        ));
        assertThat(analysis.sourceMaps().size(), is(3));

        assertThat((Integer) analysis.sourceMaps().get(0)[0], is(1));
        assertThat((BytesRef) analysis.sourceMaps().get(0)[1], is(new BytesRef("Trillian")));

        assertThat((Integer) analysis.sourceMaps().get(1)[0], is(2));
        assertThat((BytesRef) analysis.sourceMaps().get(1)[1], is(new BytesRef("Ford")));

        assertThat((Integer) analysis.sourceMaps().get(2)[0], is(3));
        assertThat((BytesRef) analysis.sourceMaps().get(2)[1], is(new BytesRef("Zaphod")));

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
        expectedException.expectMessage("Validation failed for awesome: Invalid value of type 'MATCH_PREDICATE' in insert statement");
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
        assertThat(analysis.generatePartitions(), contains(
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
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1L));
        assertThat((Long) analysis.sourceMaps().get(1)[0], is(2L));
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
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1L));
        assertThat((Long) analysis.sourceMaps().get(1)[0], is(2L));
    }

    @Test
    public void testInsertWithBulkArgsTypeMissMatch() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for id: 11!");
        analyze("insert into users (id, name) values (?, ?)",
                new Object[][]{
                        new Object[]{10, "foo"},
                        new Object[]{"11!", "bar"}
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
        assertEquals(analysis.sourceMaps().get(0)[1], new BytesRef("foo"));
        assertEquals(analysis.sourceMaps().get(1)[1], null);
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
        assertEquals(analysis.sourceMaps().get(0)[1], null);
        assertEquals(analysis.sourceMaps().get(1)[1], new BytesRef("foo"));
    }

    @Test
    public void testInsertWithBulkArgsArrayNullValuesFirst() throws Exception {
        InsertFromValuesAnalyzedStatement analysis;
        analysis = (InsertFromValuesAnalyzedStatement) analyze("insert into users (id, new_col) values (?, ?)",
                new Object[][] {
                        new Object[] { 12, new String[]{ null }  },
                        new Object[] { 13, new String[]{ "foo" } },
                });

        assertThat(analysis.sourceMaps().size(), is(2));
        assertThat((Object[])analysis.sourceMaps().get(0)[1], arrayContaining((Object) null));
        assertThat((Object[]) analysis.sourceMaps().get(1)[1], arrayContaining((Object) new BytesRef("foo")));
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
                new Object[]{1, "Arthur"},
                new Object[]{new Foo(), "Ford"},
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

    @Test
    public void testInvalidColumnName() throws Exception {
        expectedException.expect(InvalidColumnNameException.class);
        expectedException.expectMessage("column name \"newCol[\" is invalid");
        analyze("insert into users (\"newCol[\") values(test)");
    }

    @Test
    public void testInsertIntoTableWithNestedObjectPrimaryKeyAndNullInsert() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Primary key value must not be NULL");
        analyze("insert into nested_pk (o) values (null)");
    }

    @Test
    public void testNestedPrimaryKeyColumnMustNotBeNull() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Primary key value must not be NULL");
        analyze("insert into nested_pk (o) values ({b=null})");
    }

    @Test
    public void testNestedClusteredByColumnMustNotBeNull() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Clustered by value must not be NULL");
        analyze("insert into nested_clustered (o) values ({c=null})");
    }

    @Test
    public void testNestedClusteredByColumnMustNotBeNullWholeObject() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Clustered by value must not be NULL");
        analyze("insert into nested_clustered (o) values (null)");
    }

    @Test
    public void testInsertIntoTableWithNestedPartitionedByColumnAndNullValue() throws Exception {
        // caused an AssertionError before... now there should be an entry with value null in the partition map
        InsertFromValuesAnalyzedStatement statement = ((InsertFromValuesAnalyzedStatement) analyze(
                "insert into nested_parted (obj) values (null)"));
        assertThat(statement.partitionMaps().get(0).containsKey("obj.name"), is(true));
        assertThat(statement.partitionMaps().get(0).get("obj.name"), nullValue());
    }

    @Test
    public void testInsertFromValuesWithOnDuplicateKey() throws Exception {
        InsertFromValuesAnalyzedStatement statement = (InsertFromValuesAnalyzedStatement) analyze(
                "insert into users (id, name, other_id) values (1, 'Arthur', 10) " +
                "on duplicate key update name = substr(values (name), 1, 2), " +
                "other_id = other_id + 100");
        assertThat(statement.onDuplicateKeyAssignments().size(), is(1));

        Symbol[] assignments = statement.onDuplicateKeyAssignments().get(0);
        assertThat(assignments.length, is(2));
        assertLiteralSymbol(assignments[0], "Ar");
        assertThat(assignments[1], isFunction(AddFunction.NAME));
        Function function = (Function)assignments[1];
        assertThat(function.arguments().get(0), isReference("other_id"));
    }

    @Test
    public void testInsertFromValuesWithOnDuplicateKeyInvalidColumnInValues() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column does_not_exist unknown");
        analyze("insert into users (id, name) values (1, 'Arthur') " +
                "on duplicate key update name = values (does_not_exist)");
    }

    @Test
    public void testInsertFromValuesWithOnDuplicateKeyFunctionInValues() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
                "Argument to VALUES expression must reference a column that is part of the INSERT statement. random() is invalid");
        analyze("insert into users (id, name) values (1, 'Arthur') " +
                "on duplicate key update name = values (random())");
    }

    @Test
    public void testInsertFromValuesWithOnDupKeyValuesWithNotInsertedColumnRef() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Referenced column 'name' isn't part of the column list of the INSERT statement");
        analyze("insert into users (id) values (1) on duplicate key update name = values(name)");
    }

    @Test
    public void testInsertFromValuesWithOnDupKeyValuesWithReferenceToNull() throws Exception {
        InsertFromValuesAnalyzedStatement statement = (InsertFromValuesAnalyzedStatement) analyze(
            "insert into users (id, name) values (1, null) on duplicate key update name = values(name)");
        assertThat(statement.onDuplicateKeyAssignments().size(), is(1));
        Symbol[] assignments = statement.onDuplicateKeyAssignments().get(0);
        assertThat(assignments.length, is(1));
        assertThat(assignments[0], isLiteral(null, DataTypes.STRING));
    }

    @Test
    public void testInsertFromValuesWithOnDupKeyValuesWithParams() throws Exception {
        InsertFromValuesAnalyzedStatement statement = (InsertFromValuesAnalyzedStatement) analyze(
                "insert into users (id, name) values (1, ?) on duplicate key update name = values(name)",
                new Object[] { "foobar"});
        assertThat(statement.onDuplicateKeyAssignments().size(), is(1));
        Symbol[] assignments = statement.onDuplicateKeyAssignments().get(0);
        assertThat(assignments.length, is(1));
        assertLiteralSymbol(assignments[0], "foobar");
    }

    @Test
    public void testInsertFromValuesWithOnDuplicateWithTwoRefsAndDifferentTypes() throws Exception {
        InsertFromValuesAnalyzedStatement statement = (InsertFromValuesAnalyzedStatement) analyze(
                "insert into users (id, name) values (1, 'foobar') " +
                        "on duplicate key update name = awesome");
        assertThat(statement.onDuplicateKeyAssignments().size(), is(1));
        Symbol symbol = statement.onDuplicateKeyAssignments().get(0)[0];
        assertThat(symbol, isFunction("toString"));
    }

    @Test
    public void testInsertFromMultipleValuesWithOnDuplicateKey() throws Exception {
        InsertFromValuesAnalyzedStatement statement = (InsertFromValuesAnalyzedStatement) analyze(
                "insert into users (id, name) values (1, 'Arthur'), (2, 'Trillian') " +
                "on duplicate key update name = substr(values (name), 1, 1)");
        assertThat(statement.onDuplicateKeyAssignments().size(), is(2));

        Symbol[] assignments = statement.onDuplicateKeyAssignments().get(0);
        assertThat(assignments.length, is(1));
        assertLiteralSymbol(assignments[0], "A");

        assignments = statement.onDuplicateKeyAssignments().get(1);
        assertThat(assignments.length, is(1));
        assertLiteralSymbol(assignments[0], "T");
    }

    @Test
    public void testOnDuplicateKeyUpdateOnObjectColumn() throws Exception {
        InsertFromValuesAnalyzedStatement statement = (InsertFromValuesAnalyzedStatement) analyze(
                "insert into users (id) values (1) on duplicate key update details['foo'] = 'foobar'");
        assertThat(statement.onDuplicateKeyAssignments().size(), is(1));
        Symbol[] assignments = statement.onDuplicateKeyAssignments().get(0);
        assertThat(assignments.length, is(1));
        assertLiteralSymbol(assignments[0], "foobar");
    }

    @Test
    public void testInvalidLeftSideExpressionInOnDuplicateKey() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        analyze("insert into users (id, name) values (1, 'Arthur') on duplicate key update [1, 2] = 1");
    }

    @Test
    public void testUpdateOnPartitionedColumnShouldRaiseAnError() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Updating a partitioned-by column is not supported");
        analyze("insert into parted (id) values (1) on duplicate key update date = 0");
    }

    @Test
    public void testUpdateOnPrimaryKeyColumnShouldRaiseAnError() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Updating a primary key is not supported");
        analyze("insert into nested_pk (id, o) values (1, {b=1}) on duplicate key update id = 10");
    }

    @Test
    public void testUpdateOnClusteredByColumnShouldRaiseAnError() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Updating a clustered-by column is not supported");
        analyze("insert into users (id) values (1) on duplicate key update id = 10");
    }
}

