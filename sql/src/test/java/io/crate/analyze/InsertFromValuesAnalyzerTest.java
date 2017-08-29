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

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.InvalidColumnNameException;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference.IndexType;
import io.crate.metadata.Routing;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.scalar.arithmetic.ArithmeticFunctions;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.MapBuilder;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.crate.analyze.TableDefinitions.SHARD_ROUTING;
import static io.crate.analyze.TableDefinitions.USER_TABLE_IDENT;
import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class InsertFromValuesAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private static final TableIdent TEST_ALIAS_TABLE_IDENT = new TableIdent(Schemas.DOC_SCHEMA_NAME, "alias");
    private static final DocTableInfo TEST_ALIAS_TABLE_INFO = new TestingTableInfo.Builder(
        TEST_ALIAS_TABLE_IDENT, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
        .add("bla", DataTypes.STRING, null)
        .isAlias(true).build();

    private static final TableIdent NESTED_CLUSTERED_TABLE_IDENT = new TableIdent(Schemas.DOC_SCHEMA_NAME, "nested_clustered");
    private static final DocTableInfo NESTED_CLUSTERED_TABLE_INFO = new TestingTableInfo.Builder(
        NESTED_CLUSTERED_TABLE_IDENT, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
        .add("o", DataTypes.OBJECT, null)
        .add("o", DataTypes.STRING, Arrays.asList("c"))
        .clusteredBy("o.c")
        .build();

    private static final TableIdent THREE_PK_TABLE_IDENT = new TableIdent(Schemas.DOC_SCHEMA_NAME, "three_pk");
    private static final DocTableInfo THREE_PK_TABLE_INFO = new TestingTableInfo.Builder(
        THREE_PK_TABLE_IDENT, new Routing(ImmutableMap.of()))
        .add("a", DataTypes.INTEGER)
        .add("b", DataTypes.INTEGER)
        .add("c", DataTypes.INTEGER)
        .addPrimaryKey("a")
        .addPrimaryKey("b")
        .addPrimaryKey("c")
        .build();

    private SQLExecutor e;

    @Before
    public void prepare() {
        SQLExecutor.Builder executorBuilder = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .addDocTable(TEST_ALIAS_TABLE_INFO)
            .addDocTable(NESTED_CLUSTERED_TABLE_INFO)
            .addDocTable(THREE_PK_TABLE_INFO);

        TableIdent notNullColumnTableIdent = new TableIdent(Schemas.DOC_SCHEMA_NAME, "not_null_column");
        TestingTableInfo.Builder notNullColumnTable = new TestingTableInfo.Builder(
            notNullColumnTableIdent, new Routing(ImmutableMap.of()))
            .add("id", DataTypes.INTEGER, null)
            .add("name", DataTypes.STRING, null, ColumnPolicy.DYNAMIC, IndexType.NOT_ANALYZED, false, false);
        executorBuilder.addDocTable(notNullColumnTable);

        TableIdent generatedColumnTableIdent = new TableIdent(Schemas.DOC_SCHEMA_NAME, "generated_column");
        TestingTableInfo.Builder generatedColumnTable = new TestingTableInfo.Builder(
            generatedColumnTableIdent, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
            .add("ts", DataTypes.TIMESTAMP, null)
            .add("user", DataTypes.OBJECT, null)
            .add("user", DataTypes.STRING, Arrays.asList("name"))
            .addGeneratedColumn("day", DataTypes.TIMESTAMP, "date_trunc('day', ts)", false)
            .addGeneratedColumn("name", DataTypes.STRING, "concat(\"user\"['name'], 'bar')", false);
        executorBuilder.addDocTable(generatedColumnTable);

        TableIdent generatedPkColumnTableIdent = new TableIdent(Schemas.DOC_SCHEMA_NAME, "generated_pk_column");
        TestingTableInfo.Builder generatedPkColumnTable = new TestingTableInfo.Builder(
            generatedPkColumnTableIdent, SHARD_ROUTING)
            .add("serial_no", DataTypes.INTEGER, null)
            .add("product_no", DataTypes.INTEGER, null)
            .add("color", DataTypes.STRING, null)
            .addGeneratedColumn("id", DataTypes.INTEGER, "serial_no + 1", false)
            .addGeneratedColumn("id2", DataTypes.INTEGER, "product_no + 1", false)
            .addPrimaryKey("id")
            .addPrimaryKey("id2");
        executorBuilder.addDocTable(generatedPkColumnTable);

        TableIdent generatedClusteredByTableIdent = new TableIdent(Schemas.DOC_SCHEMA_NAME, "generated_clustered_by_column");
        TestingTableInfo.Builder clusteredByGeneratedTable = new TestingTableInfo.Builder(
            generatedClusteredByTableIdent, SHARD_ROUTING)
            .add("serial_no", DataTypes.INTEGER, null)
            .add("color", DataTypes.STRING, null)
            .addGeneratedColumn("routing_col", DataTypes.INTEGER, "serial_no + 1", false)
            .clusteredBy("routing_col");
        executorBuilder.addDocTable(clusteredByGeneratedTable);

        TableIdent generatedNestedClusteredByTableIdent = new TableIdent(Schemas.DOC_SCHEMA_NAME, "generated_nested_clustered_by");
        TestingTableInfo.Builder generatedNestedClusteredByInfo = new TestingTableInfo.Builder(
            generatedNestedClusteredByTableIdent, SHARD_ROUTING)
            .add("o", DataTypes.OBJECT, null, ColumnPolicy.DYNAMIC)
            .add("o", DataTypes.INTEGER, Arrays.asList("serial_number"))
            .addGeneratedColumn("routing_col", DataTypes.INTEGER, "o['serial_number'] + 1", false)
            .clusteredBy("routing_col");
        executorBuilder.addDocTable(generatedNestedClusteredByInfo);

        e = executorBuilder.build();
    }

    @Test
    public void testInsertWithColumns() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze("insert into users (id, name) values (1, 'Trillian')");
        assertThat(analysis.tableInfo().ident(), is(USER_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());

        assertThat(analysis.columns().get(1).ident().columnIdent().name(), is("name"));
        assertEquals(DataTypes.STRING, analysis.columns().get(1).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(analysis.sourceMaps().get(0).length, is(2));
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1L));
        assertThat((BytesRef) analysis.sourceMaps().get(0)[1], is(new BytesRef("Trillian")));
    }

    @Test
    public void testInsertWithTwistedColumns() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze("insert into users (name, id) values ('Trillian', 2)");
        assertThat(analysis.tableInfo().ident(), is(USER_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).ident().columnIdent().name(), is("name"));
        assertEquals(DataTypes.STRING, analysis.columns().get(0).valueType());

        assertThat(analysis.columns().get(1).ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(1).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(analysis.sourceMaps().get(0).length, is(2));
        assertThat((BytesRef) analysis.sourceMaps().get(0)[0], is(new BytesRef("Trillian")));
        assertThat((Long) analysis.sourceMaps().get(0)[1], is(2L));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInsertWithColumnsAndTooManyValues() throws Exception {
        e.analyze("insert into users (name, id) values ('Trillian', 2, true)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInsertWithColumnsAndTooLessValues() throws Exception {
        e.analyze("insert into users (name, id) values ('Trillian')");
    }

    @Test(expected = ValidationException.class)
    public void testInsertWithWrongType() throws Exception {
        e.analyze("insert into users (name, id) values (1, 'Trillian')");
    }

    @Test
    public void testInsertWithNumericTypeOutOfRange() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for bytes: 1234 cannot be cast to type byte");
        e.analyze("insert into users (name, id, bytes) values ('Trillian', 4, 1234)");
    }


    @Test(expected = ValidationException.class)
    public void testInsertWithWrongParameterType() throws Exception {
        e.analyze("insert into users (name, id) values (?, ?)", new Object[]{1, true});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInsertSameReferenceRepeated() throws Exception {
        e.analyze("insert into users (name, name) values ('Trillian', 'Ford')");
    }

    @Test
    public void testInsertWithConvertedTypes() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze(
            "insert into users (id, name, awesome) values ($1, 'Trillian', $2)", new Object[]{1.0f, "true"});

        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());
        assertEquals(DataTypes.BOOLEAN, analysis.columns().get(2).valueType());

        assertThat(analysis.sourceMaps().get(0).length, is(3));
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1L));
        assertThat((Boolean) analysis.sourceMaps().get(0)[2], is(true));
    }

    @Test
    public void testInsertWithFunction() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze("insert into users (id, name) values (ABS(-1), 'Trillian')");
        assertThat(analysis.tableInfo().ident(), is(USER_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(2));

        assertThat(analysis.columns().get(0).ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());

        assertThat(analysis.columns().get(1).ident().columnIdent().name(), is("name"));
        assertEquals(DataTypes.STRING, analysis.columns().get(1).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(analysis.sourceMaps().get(0).length, is(2));
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1L));
        assertThat((BytesRef) analysis.sourceMaps().get(0)[1], is(new BytesRef("Trillian")));
    }

    @Test
    public void testInsertWithoutColumns() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze("insert into users values (1, 1, 'Trillian')");
        assertThat(analysis.tableInfo().ident(), is(USER_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(3));

        assertThat(analysis.columns().get(0).ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());

        assertThat(analysis.columns().get(1).ident().columnIdent().name(), is("other_id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(1).valueType());

        assertThat(analysis.columns().get(2).ident().columnIdent().name(), is("name"));
        assertEquals(DataTypes.STRING, analysis.columns().get(2).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(analysis.sourceMaps().get(0).length, is(3));
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1L));
        assertThat((Long) analysis.sourceMaps().get(0)[1], is(1L));
        assertThat((BytesRef) analysis.sourceMaps().get(0)[2], is(new BytesRef("Trillian")));
    }

    @Test
    public void testInsertWithoutColumnsAndOnlyOneColumn() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze("insert into users values (1)");
        assertThat(analysis.tableInfo().ident(), is(USER_TABLE_IDENT));
        assertThat(analysis.columns().size(), is(1));

        assertThat(analysis.columns().get(0).ident().columnIdent().name(), is("id"));
        assertEquals(DataTypes.LONG, analysis.columns().get(0).valueType());

        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(analysis.sourceMaps().get(0).length, is(1));
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1L));
    }

    @Test
    public void testInsertWithInvalidColumnReference() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid table column reference 'a'");
        InsertFromValuesAnalyzedStatement analysis = e.analyze("insert into users ('a') values (1)");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInsertIntoSysTable() throws Exception {
        e.analyze("insert into sys.nodes (id, name) values (666, 'evilNode')");
    }

    @Test
    public void testInsertIntoAliasTable() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("The relation \"doc.alias\" doesn't support or allow INSERT " +
                                        "operations, as it is read-only.");
        e.analyze("insert into alias (bla) values ('blubb')");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInsertWithoutPrimaryKey() throws Exception {
        e.analyze("insert into users (name) values ('Trillian')");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullPrimaryKey() throws Exception {
        e.analyze("insert into users (id) values (?)",
            new Object[]{null});
    }

    @Test
    public void testNullLiterals() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze("insert into users (id, name, awesome, details) values (?, ?, ?, ?)",
            new Object[]{1, null, null, null});
        assertThat(analysis.sourceMaps().get(0).length, is(4));
        assertNull(analysis.sourceMaps().get(0)[1]);
        assertNull(analysis.sourceMaps().get(0)[2]);
        assertNull(analysis.sourceMaps().get(0)[3]);
    }

    @Test
    public void testObjectLiterals() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze("insert into users (id, name, awesome, details) values (?, ?, ?, ?)",
            new Object[]{1, null, null, new HashMap<String, Object>() {{
                put("new_col", "new value");
            }}});
        assertThat(analysis.sourceMaps().get(0).length, is(4));
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1L));
        assertThat(analysis.sourceMaps().get(0)[3], instanceOf(Map.class));
    }

    @Test(expected = ColumnValidationException.class)
    public void testInsertArrays() throws Exception {
        // error because in the schema are non-array types:
        e.analyze("insert into users (id, name, awesome, details) values (?, ?, ?, ?)",
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

    public void testInsertNotNullConstraintAbsentColumn() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot insert null value for column 'name'");
        e.analyze("insert into not_null_column (id) values (1)");
    }

    @Test
    public void testInsertObjectArrayParameter() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze("insert into users (id, friends) values(?, ?)",
            new Object[]{0, new Map[]{
                new HashMap<String, Object>() {{
                    put("name", "Jeltz");
                }},
                new HashMap<String, Object>() {{
                    put("name", "Prosser");
                }}
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
        e.analyze("insert into users (id, friends) values(?, ?)",
            new Object[]{0, new Map[]{
                new HashMap<String, Object>() {{
                    put("id", "Jeltz");
                }}
            }
            });
    }

    @Test(expected = ColumnValidationException.class)
    public void testInsertInvalidObjectArrayParameter2() throws Exception {
        e.analyze("insert into users (id, friends) values(?, ?)",
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
        e.analyze("insert into deeply_nested (details) " +
                "values (" +
                "  {awesome=true, arguments=[1,2,3]}" +
                ")");
    }

    @Test
    public void testInsertInvalidObjectArrayFieldInObjectArray() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for tags['metadata']['id']: Invalid long");
        e.analyze("insert into deeply_nested (tags) " +
                "values (" +
                "  [" +
                "    {name='right', metadata=[{id=1}, {id=2}]}," +
                "    {name='wrong', metadata=[{id='foo'}]}" +
                "  ]" +
                ")");
    }

    @Test
    public void testInsertNestedObjectLiteral() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze(
            "insert into deeply_nested (tags) " +
            "values ([" +
            "           {\"name\"='cool', \"metadata\"=[{\"id\"=0}, {\"id\"=1}]}, " +
            "           {\"name\"='fancy', \"metadata\"=[{\"id\"='2'}, {\"id\"=3}]}" +
            "         ])");
        assertThat(analysis.sourceMaps().size(), is(1));
        Object[] arrayValue = (Object[]) analysis.sourceMaps().get(0)[0];
        assertThat(arrayValue.length, is(2));
        assertThat(arrayValue[0], instanceOf(Map.class));
        assertThat((BytesRef) ((Map) arrayValue[0]).get("name"), is(new BytesRef("cool")));
        assertThat((BytesRef) ((Map) arrayValue[1]).get("name"), is(new BytesRef("fancy")));
        assertThat(Arrays.toString(((Object[]) ((Map) arrayValue[0]).get("metadata"))), is("[{id=0}, {id=1}]"));
        assertThat(Arrays.toString(((Object[]) ((Map) arrayValue[1]).get("metadata"))), is("[{id=2}, {id=3}]"));
    }

    @Test
    public void testInsertEmptyObjectArrayParameter() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze("insert into users (id, friends) values(?, ?)",
            new Object[]{0, new Map[0]});
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(0L));
        assertThat(((Object[]) analysis.sourceMaps().get(0)[1]).length, is(0));
    }

    @Test(expected = InvalidColumnNameException.class)
    public void testInsertSystemColumn() throws Exception {
        e.analyze("insert into users (id, _id) values (?, ?)",
            new Object[]{1, "1"});
    }

    @Test
    public void testNestedPk() throws Exception {
        // FYI: insert nested clustered by test here too
        InsertFromValuesAnalyzedStatement analysis = e.analyze("insert into nested_pk (id, o) values (?, ?)",
            new Object[]{1, new MapBuilder<String, Object>().put("b", 4).map()});
        assertThat(analysis.ids().size(), is(1));
        assertThat(analysis.ids().get(0),
            is(generateId(Arrays.asList(new ColumnIdent("id"), new ColumnIdent("o.b")), Arrays.asList(new BytesRef("1"), new BytesRef("4")), new ColumnIdent("o.b"))));
    }

    @Test
    public void testNestedPkAllColumns() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze("insert into nested_pk values (?, ?)",
            new Object[]{1, new MapBuilder<String, Object>().put("b", 4).map()});
        assertThat(analysis.ids().size(), is(1));
        assertThat(analysis.ids().get(0),
            is(generateId(Arrays.asList(new ColumnIdent("id"), new ColumnIdent("o.b")), Arrays.asList(new BytesRef("1"), new BytesRef("4")), new ColumnIdent("o.b"))));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingNestedPk() throws Exception {
        e.analyze("insert into nested_pk (id) values (?)", new Object[]{1});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingNestedPkInMap() throws Exception {
        e.analyze("insert into nested_pk (id, o) values (?, ?)", new Object[]{1, new HashMap<String, Object>()});
    }

    @Test
    public void testTwistedNestedPk() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze("insert into nested_pk (o, id) values (?, ?)",
            new Object[]{new MapBuilder<String, Object>().put("b", 4).map(), 1});
        assertThat(analysis.ids().get(0),
            is(generateId(Arrays.asList(new ColumnIdent("id"), new ColumnIdent("o.b")), Arrays.asList(new BytesRef("1"), new BytesRef("4")), new ColumnIdent("o.b"))));

    }

    private String generateId(List<ColumnIdent> pkColumns, List<BytesRef> pkValues, ColumnIdent clusteredBy) {
        return Id.compileWithNullValidation(pkColumns, clusteredBy).apply(pkValues);
    }

    @Test
    public void testInsertMultipleValues() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze(
            "insert into users (id, name, awesome) values (?, ?, ?), (?, ?, ?)",
            new Object[]{99, "Marvin", true, 42, "Deep Thought", false});
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
        InsertFromValuesAnalyzedStatement analysis = e.analyze(
            "insert into parted (id, name, date) " +
            "values (?, ?, ?)", new Object[]{0, "Trillian", 0L});
        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(analysis.sourceMaps().get(0).length, is(3));
        assertThat(analysis.columns().size(), is(3));
        assertThat(analysis.partitionMaps().size(), is(1));
        assertThat(analysis.partitionMaps().get(0), hasEntry("date", "0"));
    }

    @Test
    public void testInsertIntoPartitionedTableOnlyPartitionColumns() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze(
            "insert into parted (date) " +
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
        InsertFromValuesAnalyzedStatement analysis = e.analyze(
            "insert into parted (id, name, date) " +
            "values (?, ?, ?), (?, ?, ?), (?, ?, ?)",
            new Object[]{
                1, "Trillian", 13963670051500L,
                2, "Ford", 0L,
                3, "Zaphod", null
            });
        validateBulkIndexPartitionedTableAnalysis(analysis);
        // bulk args
        analysis = e.analyze("insert into parted (id, name, date) " +
                           "values (?, ?, ?)",
            new Object[][]{
                new Object[]{1, "Trillian", 13963670051500L},
                new Object[]{2, "Ford", 0L},
                new Object[]{3, "Zaphod", null}
            });
        validateBulkIndexPartitionedTableAnalysis(analysis);
    }

    private void validateBulkIndexPartitionedTableAnalysis(InsertFromValuesAnalyzedStatement analysis) {
        assertThat(analysis.generatePartitions(), contains(
            new PartitionName("parted", Arrays.asList(new BytesRef("13963670051500"))).asIndexName(),
            new PartitionName("parted", Arrays.asList(new BytesRef("0"))).asIndexName(),
            new PartitionName("parted", new ArrayList<BytesRef>() {{
                add(null);
            }}).asIndexName()
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
        expectedException.expectMessage("Validation failed for awesome: " +
                                        "Invalid value 'match({\"name\"=NULL}, 'bar', 'best_fields', {})' in insert statement");
        e.analyze("insert into users (id, awesome) values (1, match(name, 'bar'))");
    }

    @Test
    public void testInsertNestedPartitionedColumn() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze(
            "insert into nested_parted (id, date, obj)" +
            "values (?, ?, ?), (?, ?, ?)",
            new Object[]{
                1, "1970-01-01", new MapBuilder<String, Object>().put("name", "Zaphod").map(),
                2, "2014-05-21", new MapBuilder<String, Object>().put("name", "Arthur").map()
            });
        assertThat(analysis.generatePartitions(), contains(
            new PartitionName("nested_parted", Arrays.asList(new BytesRef("0"), new BytesRef("Zaphod"))).asIndexName(),
            new PartitionName("nested_parted", Arrays.asList(new BytesRef("1400630400000"), new BytesRef("Arthur"))).asIndexName()

        ));
        assertThat(analysis.sourceMaps().size(), is(2));
    }

    @Test
    public void testInsertWithBulkArgs() throws Exception {
        InsertFromValuesAnalyzedStatement analysis;
        analysis = e.analyze(
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
        analysis = e.analyze(
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
        expectedException.expectMessage("Validation failed for id: '11!' cannot be cast to type long");
        e.analyze("insert into users (id, name) values (?, ?)",
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
        e.analyze("insert into users (id, name) values (?, ?)",
            new Object[][]{
                new Object[]{10, "foo"},
                new Object[]{"11"}
            }
        );
    }

    @Test
    public void testInsertWithBulkArgsNullValues() throws Exception {
        InsertFromValuesAnalyzedStatement analysis;
        analysis = e.analyze("insert into users (id, name) values (?, ?)",
            new Object[][]{
                new Object[]{10, "foo"},
                new Object[]{11, null}
            });

        assertThat(analysis.sourceMaps().size(), is(2));
        assertEquals(analysis.sourceMaps().get(0)[1], new BytesRef("foo"));
        assertEquals(analysis.sourceMaps().get(1)[1], null);
    }

    @Test
    public void testInsertWithBulkArgsNullValuesFirst() throws Exception {
        InsertFromValuesAnalyzedStatement analysis;
        analysis = e.analyze("insert into users (id, name) values (?, ?)",
            new Object[][]{
                new Object[]{12, null},
                new Object[]{13, "foo"},
            });

        assertThat(analysis.sourceMaps().size(), is(2));
        assertEquals(analysis.sourceMaps().get(0)[1], null);
        assertEquals(analysis.sourceMaps().get(1)[1], new BytesRef("foo"));
    }

    @Test
    public void testInsertWithBulkArgsArrayNullValuesFirst() throws Exception {
        InsertFromValuesAnalyzedStatement analysis;
        analysis = e.analyze("insert into users (id, new_col) values (?, ?)",
            new Object[][]{
                new Object[]{12, new String[]{null}},
                new Object[]{13, new String[]{"foo"}},
            });

        assertThat(analysis.sourceMaps().size(), is(2));
        assertThat((Object[]) analysis.sourceMaps().get(0)[1], arrayContaining((Object) null));
        assertThat((Object[]) analysis.sourceMaps().get(1)[1], arrayContaining((Object) new BytesRef("foo")));
    }

    @Test
    public void testInsertBulkArgWithFirstArgsContainsUnrecognizableObject() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(Matchers.allOf(
            Matchers.startsWith("Got an argument \""),
            Matchers.endsWith("that couldn't be recognized")
        ));

        e.analyze("insert into users (id, name) values (?, ?)", new Object[][]{
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

        e.analyze("insert into users (id, name) values (?, ?)", new Object[][]{
            new Object[]{1, "Arthur"},
            new Object[]{new Foo(), "Ford"},
        });
    }

    private static class Foo {}


    @Test
    public void testInsertWithTooFewArguments() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Tried to resolve a parameter but the arguments provided with the SQLRequest don't contain a parameter at position 1");
        e.analyze("insert into users (id, name) values (?, ?)", new Object[]{1});
    }

    @Test
    public void testInsertWithTooFewBulkArguments() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Tried to resolve a parameter but the arguments provided with the SQLRequest don't contain a parameter at position 0");
        e.analyze("insert into users (id, name) values (?, ?)", new Object[][]{
            new Object[]{},
            new Object[]{}
        });
    }

    @Test
    public void testInvalidTypeParamLiteral() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for tags: [['the', 'answer'], ['what''s', 'the', " +
                                        "'question', '?']] cannot be cast to type string_array");

        e.analyze("insert into users (id, name, tags) values (42, 'Deep Thought', [['the', 'answer'], ['what''s', 'the', 'question', '?']])");

    }

    @Test
    public void testInvalidTypeParam() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for tags: [['the', 'answer'], ['what''s', 'the', " +
                                        "'question', '?']] cannot be cast to type string_array");

        e.analyze("insert into users (id, name, tags) values (42, 'Deep Thought', ?)", new Object[]{
            new String[][]{
                new String[]{"the", "answer"},
                new String[]{"what's", "the", "question", "?"}
            }
        });

    }

    @Test
    public void testInvalidTypeBulkParam() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for tags: [['the', 'answer'], ['what''s', 'the', " +
                                        "'question', '?']] cannot be cast to type string_array");

        e.analyze("insert into users (id, name, tags) values (42, 'Deep Thought', ?)", new Object[][]{
            new Object[]{
                new String[][]{
                    new String[]{"the", "answer"},
                    new String[]{"what's", "the", "question", "?"}
                }
            }
        });
    }

    @Test
    public void testDynamicNestedArrayParamLiteral() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze("insert into users (id, name, theses) " +
                                                             "values (1, 'Marx', [['string1', 'string2']])");
        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1L));
        assertThat((BytesRef) analysis.sourceMaps().get(0)[1], is(new BytesRef("Marx")));
        assertThat((Object[]) ((Object[]) analysis.sourceMaps().get(0)[2])[0],
            arrayContaining(new Object[]{new BytesRef("string1"), new BytesRef("string2")}));
    }

    @Test
    public void testDynamicNestedArrayParam() throws Exception {
        e.analyze("insert into users (id, name, theses) values (1, 'Marx', ?)", new Object[]{
            new String[][]{
                new String[]{"string1"},
                new String[]{"string2"}
            }
        });
        InsertFromValuesAnalyzedStatement analysis = e.analyze("insert into users (id, name, theses) " +
                                                             "values (1, 'Marx', [['string1', 'string2']])");
        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1L));
        assertThat((BytesRef) analysis.sourceMaps().get(0)[1], is(new BytesRef("Marx")));
        assertThat((Object[]) ((Object[]) analysis.sourceMaps().get(0)[2])[0],
            arrayContaining(new Object[]{new BytesRef("string1"), new BytesRef("string2")}));
    }

    @Test
    public void testDynamicNestedArrayBulkParam() throws Exception {
        e.analyze("insert into users (id, name, theses) values (1, 'Marx', ?)", new Object[][]{
            new Object[]{
                new String[][]{
                    new String[]{"string1"},
                    new String[]{"string2"}
                }
            }
        });
        InsertFromValuesAnalyzedStatement analysis = e.analyze("insert into users (id, name, theses) " +
                                                             "values (1, 'Marx', [['string1', 'string2']])");
        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1L));
        assertThat((BytesRef) analysis.sourceMaps().get(0)[1], is(new BytesRef("Marx")));
        assertThat((Object[]) ((Object[]) analysis.sourceMaps().get(0)[2])[0],
            arrayContaining(new Object[]{new BytesRef("string1"), new BytesRef("string2")}));
    }

    @Test
    public void testInvalidColumnName() throws Exception {
        expectedException.expect(InvalidColumnNameException.class);
        expectedException.expectMessage(
            "\"newCol['a']\" conflicts with subscript pattern");
        e.analyze("insert into users (\"newCol['a']\") values(test)");
    }

    @Test
    public void testInsertIntoTableWithNestedObjectPrimaryKeyAndNullInsert() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Primary key value must not be NULL");
        e.analyze("insert into nested_pk (o) values (null)");
    }

    @Test
    public void testNestedPrimaryKeyColumnMustNotBeNull() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Primary key value must not be NULL");
        e.analyze("insert into nested_pk (o) values ({b=null})");
    }

    @Test
    public void testNestedClusteredByColumnMustNotBeNull() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Clustered by value must not be NULL");
        e.analyze("insert into nested_clustered (o) values ({c=null})");
    }

    @Test
    public void testNestedClusteredByColumnMustNotBeNullWholeObject() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Clustered by value must not be NULL");
        e.analyze("insert into nested_clustered (o) values (null)");
    }

    @Test
    public void testInsertIntoTableWithNestedPartitionedByColumnAndNullValue() throws Exception {
        // caused an AssertionError before... now there should be an entry with value null in the partition map
        InsertFromValuesAnalyzedStatement statement = e.analyze(
            "insert into nested_parted (obj) values (null)");
        assertThat(statement.partitionMaps().get(0).containsKey("obj.name"), is(true));
        assertThat(statement.partitionMaps().get(0).get("obj.name"), nullValue());
    }

    @Test
    public void testInsertFromValuesWithOnDuplicateKey() throws Exception {
        InsertFromValuesAnalyzedStatement statement = e.analyze(
            "insert into users (id, name, other_id) values (1, 'Arthur', 10) " +
            "on duplicate key update name = substr(values (name), 1, 2), " +
            "other_id = other_id + 100");
        assertThat(statement.onDuplicateKeyAssignments().size(), is(1));

        Symbol[] assignments = statement.onDuplicateKeyAssignments().get(0);
        assertThat(assignments.length, is(2));
        assertThat(assignments[0], isLiteral("Ar"));
        assertThat(assignments[1], isFunction(ArithmeticFunctions.Names.ADD));
        Function function = (Function) assignments[1];
        assertThat(function.arguments().get(0), isReference("other_id"));
    }

    @Test
    public void testInsertFromValuesWithOnDuplicateKeyInvalidColumnInValues() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column does_not_exist unknown");
       e.analyze("insert into users (id, name) values (1, 'Arthur') " +
                "on duplicate key update name = values (does_not_exist)");
    }

    @Test
    public void testInsertFromValuesWithOnDuplicateKeyFunctionInValues() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "Argument to VALUES expression must reference a column that is part of the INSERT statement. random() is invalid");
       e.analyze("insert into users (id, name) values (1, 'Arthur') " +
                "on duplicate key update name = values (random())");
    }

    @Test
    public void testInsertFromValuesWithOnDupKeyValuesWithNotInsertedColumnRef() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Referenced column 'name' isn't part of the column list of the INSERT statement");
       e.analyze("insert into users (id) values (1) on duplicate key update name = values(name)");
    }

    @Test
    public void testInsertFromValuesWithOnDupKeyValuesWithReferenceToNull() throws Exception {
        InsertFromValuesAnalyzedStatement statement = e.analyze(
            "insert into users (id, name) values (1, null) on duplicate key update name = values(name)");
        assertThat(statement.onDuplicateKeyAssignments().size(), is(1));
        Symbol[] assignments = statement.onDuplicateKeyAssignments().get(0);
        assertThat(assignments.length, is(1));
        assertThat(assignments[0], isLiteral(null, DataTypes.STRING));
    }

    @Test
    public void testInsertFromValuesWithOnDupKeyValuesWithParams() throws Exception {
        InsertFromValuesAnalyzedStatement statement = e.analyze(
            "insert into users (id, name) values (1, ?) on duplicate key update name = values(name)",
            new Object[]{"foobar"});
        assertThat(statement.onDuplicateKeyAssignments().size(), is(1));
        Symbol[] assignments = statement.onDuplicateKeyAssignments().get(0);
        assertThat(assignments.length, is(1));
        assertThat(assignments[0], isLiteral("foobar"));
    }

    @Test
    public void testInsertFromValuesWithOnDuplicateWithTwoRefsAndDifferentTypes() throws Exception {
        InsertFromValuesAnalyzedStatement statement = e.analyze(
            "insert into users (id, name) values (1, 'foobar') " +
            "on duplicate key update name = awesome");
        assertThat(statement.onDuplicateKeyAssignments().size(), is(1));
        Symbol symbol = statement.onDuplicateKeyAssignments().get(0)[0];
        assertThat(symbol, isFunction("to_string"));
    }

    @Test
    public void testInsertFromMultipleValuesWithOnDuplicateKey() throws Exception {
        InsertFromValuesAnalyzedStatement statement = e.analyze(
            "insert into users (id, name) values (1, 'Arthur'), (2, 'Trillian') " +
            "on duplicate key update name = substr(values (name), 1, 1)");
        assertThat(statement.onDuplicateKeyAssignments().size(), is(2));

        Symbol[] assignments = statement.onDuplicateKeyAssignments().get(0);
        assertThat(assignments.length, is(1));
        assertThat(assignments[0], isLiteral("A"));

        assignments = statement.onDuplicateKeyAssignments().get(1);
        assertThat(assignments.length, is(1));
        assertThat(assignments[0], isLiteral("T"));
    }

    @Test
    public void testOnDuplicateKeyUpdateOnObjectColumn() throws Exception {
        InsertFromValuesAnalyzedStatement statement = e.analyze(
            "insert into users (id) values (1) on duplicate key update details['foo'] = 'foobar'");
        assertThat(statement.onDuplicateKeyAssignments().size(), is(1));
        Symbol[] assignments = statement.onDuplicateKeyAssignments().get(0);
        assertThat(assignments.length, is(1));
        assertThat(assignments[0], isLiteral("foobar"));
    }

    @Test
    public void testInvalidLeftSideExpressionInOnDuplicateKey() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
       e.analyze("insert into users (id, name) values (1, 'Arthur') on duplicate key update [1, 2] = 1");
    }

    @Test
    public void testUpdateOnPartitionedColumnShouldRaiseAnError() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for date: Updating a partitioned-by column is not supported");
       e.analyze("insert into parted (id) values (1) on duplicate key update date = 0");
    }

    @Test
    public void testUpdateOnPrimaryKeyColumnShouldRaiseAnError() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for id: Updating a primary key is not supported");
       e.analyze("insert into nested_pk (id, o) values (1, {b=1}) on duplicate key update id = 10");
    }

    @Test
    public void testUpdateOnClusteredByColumnShouldRaiseAnError() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for id: Updating a clustered-by column is not supported");
       e.analyze("insert into users (id) values (1) on duplicate key update id = 10");
    }

    @Test
    public void testInsertWithGeneratedColumn() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze(
            "insert into generated_column (ts) values (?)",
            new Object[]{"2015-11-18T11:11:00"});
        assertThat(analysis.sourceMaps().size(), is(1));

        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1447845060000L));
        assertThat((Long) analysis.sourceMaps().get(0)[1], is(1447804800000L));
    }

    @Test
    public void testInsertWithGeneratedColumnReferenceValueNullOrNotGiven() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze(
            "insert into generated_column (ts) values (?)",
            new Object[]{null});
        assertThat(analysis.sourceMaps().size(), is(1));

        Object[] values = analysis.sourceMaps().get(0);
        assertThat(values.length, is(3));

        assertThat(values[0], nullValue());
        // generated column 'day'
        assertThat(values[1], nullValue());
        // generated column 'name'
        assertThat((BytesRef) values[2], is(new BytesRef("bar")));
    }

    @Test
    public void testInsertWithGeneratedColumnWithValueGiven() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze(
            "insert into generated_column (ts, day) values (?, ?)",
            new Object[]{"2015-11-18T11:11:00", 1447804800000L});
        assertThat(analysis.sourceMaps().size(), is(1));

        assertThat((Long) analysis.sourceMaps().get(0)[0], is(1447845060000L));
        assertThat((Long) analysis.sourceMaps().get(0)[1], is(1447804800000L));
    }

    @Test
    public void testInsertWithGeneratedColumnWithInvalidValueGiven() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Given value 1449999900000 for generated column does not match defined generated expression value 1447804800000");
       e.analyze("insert into generated_column (ts, day) values (?, ?)",
            new Object[]{"2015-11-18T11:11:00", 1449999900000L});
    }

    @Test
    public void testInsertNullValueWithGeneratedColumn() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze(
            "insert into generated_column (day) values (?)",
            new Object[]{null});
        assertThat(analysis.sourceMaps().size(), is(1));
        assertThat(analysis.sourceMaps().get(0)[0], is(Matchers.nullValue()));
    }

    @Test
    public void testInsertMultipleValuesWithGeneratedColumn() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze(
            "INSERT INTO generated_column (ts, \"user\") values ('1970-01-01', {name='Johnny'}), ('1989-11-09T08:30:00', {name='Egon'})");
        assertThat(analysis.columns(), hasSize(4));
        assertThat(analysis.columns(), contains(isReference("ts"), isReference("user"), isReference("day"), isReference("name")));
        assertThat(analysis.sourceMaps(), hasSize(2));
        assertThat(analysis.sourceMaps(), contains(
            Matchers.arrayContaining(0L, ImmutableMap.<String, Object>of("name", new BytesRef("Johnny")), 0L, new BytesRef("Johnnybar")),
            Matchers.arrayContaining(626603400000L, ImmutableMap.<String, Object>of("name", new BytesRef("Egon")), 626572800000L, new BytesRef("Egonbar"))));
    }

    @Test
    public void testInsertMultipleValuesWithGeneratedColumnGiven() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze(
            "INSERT INTO generated_column (ts, \"user\", day) values ('1970-01-01', {name='Johnny'}, '1970-01-01'), ('1989-11-09T08:30:00', {name='Egon'}, '1989-11-09')");
        assertThat(analysis.columns(), hasSize(4));
        assertThat(analysis.columns(), contains(isReference("ts"), isReference("user"), isReference("day"), isReference("name")));
        assertThat(analysis.sourceMaps(), hasSize(2));
        assertThat(analysis.sourceMaps(), contains(
            Matchers.arrayContaining(0L, ImmutableMap.<String, Object>of("name", new BytesRef("Johnny")), 0L, new BytesRef("Johnnybar")),
            Matchers.arrayContaining(626603400000L, ImmutableMap.<String, Object>of("name", new BytesRef("Egon")), 626572800000L, new BytesRef("Egonbar"))));
    }

    @Test
    public void testInsertGeneratedPrimaryKeyColumn() throws Exception {
        InsertFromValuesAnalyzedStatement analysis = e.analyze(
            "INSERT INTO generated_pk_column (serial_no, product_no) values (1, 1)"
        );
        assertThat(analysis.routingValues(), contains("AgEyATI="));
        assertThat(analysis.ids().get(0),
            is(generateId(Arrays.asList(new ColumnIdent("id"), new ColumnIdent("id2")), Arrays.asList(new BytesRef("2"), new BytesRef("2")), new ColumnIdent("id"))));
    }

    @Test
    public void testInsertMultipleValuesTooManyValues() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("INSERT statement contains a VALUES clause with too many elements (3), expected (2)");
        e.analyze("INSERT INTO users (id, name) values (1, 'Johnny'), (2, 'Egon', 1234)");
    }

    @Test
    public void testInsertMultipleValuesWithGeneratedColumnAndTooFewValuesInSecondValues() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid number of values: Got 2 columns specified but 1 values");
        e.analyze("INSERT INTO generated_column (ts, username) values ('1970-01-01', {name='Johnny'}), ('1989-11-09T08:30:00')");
    }

    @Test
    public void testGeneratedPrimaryKeyMissing() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Primary key is required but is missing from the insert statement");
        e.analyze("INSERT INTO generated_pk_column (color) values ('green')");
    }

    @Test
    public void testGeneratedKeyPrimaryKeyPartMissing() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Primary key value must not be NULL");
        e.analyze("INSERT INTO generated_pk_column (serial_no) values (1)");
    }

    @Test
    public void testGeneratedClusteredByMissing() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Clustered by value is required but is missing from the insert statement");
        e.analyze("INSERT INTO generated_clustered_by_column (color) values ('black')");
    }

    @Test
    public void testNestedGeneratedClusteredByMissing() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Clustered by value is required but is missing from the insert statement");
        e.analyze("INSERT INTO generated_nested_clustered_by (name) values ('kill-o-zap blaster pistol')");
    }

    @Test
    public void testNestedGeneratedClusteredBy() throws Exception {
        InsertFromValuesAnalyzedStatement statement =
            e.analyze("INSERT INTO generated_nested_clustered_by (o) values ({serial_number=1})");
        assertThat(statement.routingValues(), contains(is("2")));
    }

    @Test
    public void testInsertArrayLiteralWithOneNullValue() throws Exception {
        InsertFromValuesAnalyzedStatement stmt = e.analyze("insert into users (id, tags) values (1, ['foo', 'bar', null])");
        assertThat(stmt.sourceMaps().get(0), is(new Object[]{1L, new Object[]{new BytesRef("foo"), new BytesRef("bar"), null}}));

        stmt = e.analyze("insert into users (id, tags) values (1, [null, 'foo', 'bar'])");
        assertThat(stmt.sourceMaps().get(0), is(new Object[]{1L, new Object[]{null, new BytesRef("foo"), new BytesRef("bar")}}));
    }

    @Test
    public void testInsertArrayLiteralWithOnlyNullValues() throws Exception {
        InsertFromValuesAnalyzedStatement stmt = e.analyze("insert into users (id, tags) values (1, [null, null])");
        assertThat(stmt.sourceMaps().get(0), is(new Object[]{1L, new Object[]{null, null}}));
    }

    @Test
    public void testIdGenerationDoesNotDependOnPrimaryKeyInsertOrder() throws Exception {
        InsertFromValuesAnalyzedStatement stmt1 = e.analyze("insert into three_pk (a, b, c) values (1, 2, 3)");
        InsertFromValuesAnalyzedStatement stmt2 = e.analyze("insert into three_pk (c, b, a) values (3, 2, 1)");
        assertThat(stmt1.ids().get(0), is(stmt2.ids().get(0)));
    }
}
