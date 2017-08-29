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
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.symbol.DynamicReference;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.Routing;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.crate.analyze.TableDefinitions.SHARD_ROUTING;
import static io.crate.analyze.TableDefinitions.USER_TABLE_INFO;
import static io.crate.testing.SymbolMatchers.*;
import static org.hamcrest.Matchers.*;

public class UpdateAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private static final String VERSION_EX_FROM_VALIDATOR = "Filtering \"_version\" in WHERE clause only works using the \"=\" operator, checking for a numeric value";

    private final TableIdent nestedClusteredByTableIdent = new TableIdent("doc", "nestedclustered");
    private final DocTableInfo nestedClusteredByTableInfo = TestingTableInfo.builder(
        nestedClusteredByTableIdent, SHARD_ROUTING)
        .add("obj", DataTypes.OBJECT, null)
        .add("obj", DataTypes.STRING, Collections.singletonList("name"))
        .add("other_obj", DataTypes.OBJECT, null)
        .clusteredBy("obj.name").build();

    private final TableIdent testAliasTableIdent = new TableIdent(Schemas.DOC_SCHEMA_NAME, "alias");
    private final DocTableInfo testAliasTableInfo = new TestingTableInfo.Builder(
        testAliasTableIdent, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
        .add("bla", DataTypes.STRING, null)
        .isAlias(true).build();

    private final TableIdent nestedPk = new TableIdent(Schemas.DOC_SCHEMA_NAME, "t_nested_pk");
    private final DocTableInfo tiNestedPk = new TestingTableInfo.Builder(
        nestedPk, SHARD_ROUTING)
        .add("o", DataTypes.OBJECT)
        .add("o", DataTypes.INTEGER, Collections.singletonList("x"))
        .add("o", DataTypes.INTEGER, Collections.singletonList("y"))
        .addPrimaryKey("o.x")
        .build();

    private SQLExecutor e;

    @Before
    public void prepare() {
        SQLExecutor.Builder builder = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .addDocTable(nestedClusteredByTableInfo)
            .addDocTable(testAliasTableInfo)
            .addDocTable(tiNestedPk);

        TableIdent partedGeneratedColumnTableIdent = new TableIdent(Schemas.DOC_SCHEMA_NAME, "parted_generated_column");
        TestingTableInfo.Builder partedGeneratedColumnTableInfo = new TestingTableInfo.Builder(
            partedGeneratedColumnTableIdent, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
            .add("ts", DataTypes.TIMESTAMP, null)
            .addGeneratedColumn("day", DataTypes.TIMESTAMP, "date_trunc('day', ts)", true);
        builder.addDocTable(partedGeneratedColumnTableInfo);

        TableIdent nestedPartedGeneratedColumnTableIdent = new TableIdent(Schemas.DOC_SCHEMA_NAME, "nested_parted_generated_column");
        TestingTableInfo.Builder nestedPartedGeneratedColumnTableInfo = new TestingTableInfo.Builder(
            nestedPartedGeneratedColumnTableIdent, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
            .add("user", DataTypes.OBJECT, null)
            .add("user", DataTypes.STRING, Arrays.asList("name"))
            .addGeneratedColumn("name", DataTypes.STRING, "concat(\"user\"['name'], 'bar')", true);
        builder.addDocTable(nestedPartedGeneratedColumnTableInfo);

        e = builder.build();
    }

    protected UpdateAnalyzedStatement analyze(String statement) {
        return e.analyze(statement);
    }

    protected UpdateAnalyzedStatement analyze(String statement, Object[] params) {
        return (UpdateAnalyzedStatement) e.analyze(statement, params);
    }

    @Test
    public void testUpdateAnalysis() throws Exception {
        AnalyzedStatement analyzedStatement = analyze("update users set name='Ford Prefect'");
        assertThat(analyzedStatement, instanceOf(UpdateAnalyzedStatement.class));
    }

    @Test(expected = TableUnknownException.class)
    public void testUpdateUnknownTable() throws Exception {
        analyze("update unknown set name='Prosser'");
    }

    @Test
    public void testUpdateSetColumnToColumnValue() throws Exception {
        UpdateAnalyzedStatement statement = analyze("update users set name=name");
        UpdateAnalyzedStatement.NestedAnalyzedStatement statement1 = statement.nestedStatements().get(0);
        assertThat(statement1.assignments().size(), is(1));
        Symbol value = statement1.assignments().entrySet().iterator().next().getValue();
        assertThat(value, isReference("name"));
    }

    @Test
    public void testUpdateSetExpression() throws Exception {
        UpdateAnalyzedStatement statement = analyze("update users set other_id=other_id+1");
        UpdateAnalyzedStatement.NestedAnalyzedStatement statement1 = statement.nestedStatements().get(0);
        assertThat(statement1.assignments().size(), is(1));
        Symbol value = statement1.assignments().entrySet().iterator().next().getValue();
        assertThat(value, isFunction("add"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateSameReferenceRepeated() throws Exception {
        analyze("update users set name='Trillian', name='Ford'");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateSameNestedReferenceRepeated() throws Exception {
        analyze("update users set details['arms']=3, details['arms']=5");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUpdateSysTables() throws Exception {
        analyze("update sys.nodes set fs=?", new Object[]{new HashMap<String, Object>() {{
            put("free", 0);
        }}});
    }

    @Test
    public void testNumericTypeOutOfRange() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for shorts: -100000 cannot be cast to type short");

        analyze("update users set shorts=-100000");
    }

    @Test
    public void testNumericOutOfRangeFromFunction() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for bytes: 1234 cannot be cast to type byte");

        analyze("update users set bytes=abs(-1234)");
    }

    @Test
    public void testUpdateAssignments() throws Exception {
        UpdateAnalyzedStatement statement = analyze("update users set name='Trillian'");
        UpdateAnalyzedStatement.NestedAnalyzedStatement statement1 = statement.nestedStatements().get(0);
        assertThat(statement1.assignments().size(), is(1));
        assertThat(((DocTableRelation) statement.sourceRelation()).tableInfo().ident(), is(new TableIdent(Schemas.DOC_SCHEMA_NAME, "users")));

        Reference ref = statement1.assignments().keySet().iterator().next();
        assertThat(ref.ident().tableIdent().name(), is("users"));
        assertThat(ref.ident().columnIdent().name(), is("name"));
        assertTrue(statement1.assignments().containsKey(ref));

        Symbol value = statement1.assignments().entrySet().iterator().next().getValue();
        assertThat(value, isLiteral("Trillian"));
    }

    @Test
    public void testUpdateAssignmentNestedDynamicColumn() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement statement =
            analyze("update users set details['arms']=3").nestedStatements().get(0);
        assertThat(statement.assignments().size(), is(1));

        Reference ref = statement.assignments().keySet().iterator().next();
        assertThat(ref, instanceOf(DynamicReference.class));
        Assert.assertEquals(DataTypes.LONG, ref.valueType());
        assertThat(ref.ident().columnIdent().isColumn(), is(false));
        assertThat(ref.ident().columnIdent().fqn(), is("details.arms"));
    }

    @Test(expected = ColumnValidationException.class)
    public void testUpdateAssignmentWrongType() throws Exception {
        analyze("update users set other_id='String'");
    }

    @Test
    public void testUpdateAssignmentConvertableType() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement statement =
            analyze("update users set other_id=9.9").nestedStatements().get(0);
        Reference ref = statement.assignments().keySet().iterator().next();
        assertThat(ref, not(instanceOf(DynamicReference.class)));
        assertEquals(DataTypes.LONG, ref.valueType());

        Symbol value = statement.assignments().entrySet().iterator().next().getValue();
        assertThat(value, isLiteral(9L));
    }

    @Test
    public void testUpdateMuchAssignments() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement statement = analyze(
            "update users set other_id=9.9, name='Trillian', details=?, stuff=true, foo='bar'",
            new Object[]{new HashMap<String, Object>()}).nestedStatements().get(0);
        assertThat(statement.assignments().size(), is(5));
    }

    @Test
    public void testNoWhereClause() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement analysis =
            analyze("update users set other_id=9").nestedStatements().get(0);
        assertThat(analysis.whereClause(), is(WhereClause.MATCH_ALL));
    }

    @Test
    public void testNoMatchWhereClause() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement analysis =
            analyze("update users set other_id=9 where true=false").nestedStatements().get(0);
        assertThat(analysis.whereClause().noMatch(), is(true));
    }

    @Test
    public void testUpdateWhereClause() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement analysis =
            analyze("update users set other_id=9 where name='Trillian'").nestedStatements().get(0);
        assertThat(analysis.whereClause().hasQuery(), is(true));
        assertThat(analysis.whereClause().noMatch(), is(false));
    }

    @Test
    public void testQualifiedNameReference() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Column reference \"users.name\" has too many parts. A column must not have a schema or a table here.");
        UpdateAnalyzedStatement.NestedAnalyzedStatement analysis =
            analyze("update users set users.name='Trillian'").nestedStatements().get(0);
    }

    @Test
    public void testUpdateWithParameter() throws Exception {
        Map[] friends = new Map[]{
            new HashMap<String, Object>() {{
                put("name", "Slartibartfast");
            }},
            new HashMap<String, Object>() {{
                put("name", "Marvin");
            }}
        };
        UpdateAnalyzedStatement.NestedAnalyzedStatement analysis =
            analyze("update users set name=?, other_id=?, friends=? where id=?",
                new Object[]{"Jeltz", 0, friends, "9"}).nestedStatements().get(0);
        assertThat(analysis.assignments().size(), is(3));
        assertThat(
            analysis.assignments().get(USER_TABLE_INFO.getReference(new ColumnIdent("name"))),
            isLiteral("Jeltz")
        );
        assertThat(
            analysis.assignments().get(USER_TABLE_INFO.getReference(new ColumnIdent("friends"))),
            isLiteral(friends, new ArrayType(DataTypes.OBJECT))
        );
        assertThat(
            analysis.assignments().get(USER_TABLE_INFO.getReference(new ColumnIdent("other_id"))),
            isLiteral(0L)
        );

        assertThat(((Function) analysis.whereClause().query()).arguments().get(1), isLiteral(9L));
    }


    @Test
    public void testUpdateWithWrongParameters() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot cast [1, 2, 3] to type long");

        analyze("update users set name=?, friends=? where other_id=?",
            new Object[]{
                new HashMap<String, Object>(),
                new Map[0],
                new Long[]{1L, 2L, 3L}}
        );
    }

    @Test
    public void testUpdateWithEmptyObjectArray() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement analysis = analyze("update users set friends=? where other_id=0",
            new Object[]{new Map[0], 0}).nestedStatements().get(0);

        Literal friendsLiteral = (Literal) analysis.assignments().get(
            USER_TABLE_INFO.getReference(new ColumnIdent("friends")));
        assertThat(friendsLiteral.valueType().id(), is(ArrayType.ID));
        assertEquals(DataTypes.OBJECT, ((ArrayType) friendsLiteral.valueType()).innerType());
        assertThat(((Object[]) friendsLiteral.value()).length, is(0));
    }

    @Test
    public void testUpdateSystemColumn() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for _id: Updating a system column is not supported");
        analyze("update users set _id=1");
    }

    @Test
    public void testUpdatePrimaryKey() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        analyze("update users set id=1");
    }

    @Test
    public void testUpdateClusteredBy() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for id: Updating a clustered-by column is not supported");
        analyze("update users_clustered_by_only set id=1");
    }

    @Test(expected = ColumnValidationException.class)
    public void testUpdatePartitionedByColumn() throws Exception {
        analyze("update parted set date = 1395874800000");
    }

    @Test
    public void testUpdatePrimaryKeyIfNestedDoesNotWork() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        analyze("update t_nested_pk set o = {y=10}");
    }

    @Test
    public void testUpdateColumnReferencedInGeneratedPartitionByColumn() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Updating a column which is referenced in a partitioned by generated column expression is not supported");
        analyze("update parted_generated_column set ts = 1449999900000");
    }

    @Test
    public void testUpdateColumnReferencedInGeneratedPartitionByColumnNestedParent() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Updating a column which is referenced in a partitioned by generated column expression is not supported");
        analyze("update nested_parted_generated_column set \"user\" = {name = 'Ford'}");
    }

    @Test
    public void testUpdateTableAlias() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement expectedAnalysis = analyze(
            "update users set awesome=true where awesome=false").nestedStatements().get(0);
        UpdateAnalyzedStatement.NestedAnalyzedStatement actualAnalysis = analyze(
            "update users as u set awesome=true where awesome=false").nestedStatements().get(0);

        assertEquals(
            expectedAnalysis.assignments(),
            actualAnalysis.assignments()
        );
        assertEquals(
            ((Function) expectedAnalysis.whereClause().query()).arguments().get(0),
            ((Function) actualAnalysis.whereClause().query()).arguments().get(0)
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateObjectArrayField() throws Exception {
        analyze("update users set friends['id'] = ?", new Object[]{
            new Long[]{1L, 2L, 3L}
        });
    }

    @Test
    public void testUpdateArrayByElement() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Updating a single element of an array is not supported");
        analyze("update users set friends[1] = 2");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWhereClauseObjectArrayField() throws Exception {
        analyze("update users set awesome=true where friends['id'] = 5");
    }

    @Test
    public void testUpdateWithFQName() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Column reference \"users.name\" has too many parts. A column must not have a schema or a table here.");
        analyze("update users set users.name = 'Ford Mustang'");
    }

    @Test
    public void testUpdateDynamicNestedArrayParamLiteral() throws Exception {
        UpdateAnalyzedStatement statement = analyze("update users set new=[[1.9, 4.8], [9.7, 12.7]]");
        DataType dataType = statement.nestedStatements().get(0).assignments().values().iterator().next().valueType();
        assertThat(dataType, is((DataType) new ArrayType(new ArrayType(DoubleType.INSTANCE))));
    }

    @Test
    public void testUpdateDynamicNestedArrayParam() throws Exception {
        UpdateAnalyzedStatement statement = analyze("update users set new=? where id=1", new Object[]{
            new Object[]{
                new Object[]{
                    1.9, 4.8
                },
                new Object[]{
                    9.7, 12.7
                }
            }
        });
        DataType dataType = statement.nestedStatements().get(0).assignments().values().iterator().next().valueType();
        assertThat(dataType, is((DataType) new ArrayType(new ArrayType(DoubleType.INSTANCE))));
    }

    @Test
    public void testUpdateInvalidType() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for tags: [['a', 'b']] cannot be cast to type string_array");
        analyze("update users set tags=? where id=1", new Object[]{
            new Object[]{
                new Object[]{"a", "b"}
            }
        });
    }

    @Test
    public void testUsingFQColumnNameShouldBePossibleInWhereClause() throws Exception {
        UpdateAnalyzedStatement statement = analyze("update users set name = 'foo' where users.name != 'foo'");
        Function eqFunction = (Function) ((Function) statement.nestedStatements().get(0).whereClause().query()).arguments().get(0);
        assertThat(eqFunction.arguments().get(0), isReference("name"));
    }

    @Test
    public void testTestUpdateOnTableWithAliasAndFQColumnNameInWhereClause() throws Exception {
        UpdateAnalyzedStatement statement = analyze("update users  t set name = 'foo' where t.name != 'foo'");
        Function eqFunction = (Function) ((Function) statement.nestedStatements().get(0).whereClause().query()).arguments().get(0);
        assertThat(eqFunction.arguments().get(0), isReference("name"));
    }

    @Test
    public void testUpdateNestedClusteredByColumn() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for obj: Updating a clustered-by column is not supported");
        analyze("update nestedclustered set obj = {name='foobar'}");
    }

    @Test
    public void testUpdateNestedClusteredByColumnWithOtherObject() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for obj: Updating a clustered-by column is not supported");
        analyze("update nestedclustered set obj = other_obj");
    }

    @Test
    public void testUpdateWhereVersionUsingWrongOperator() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(VERSION_EX_FROM_VALIDATOR);
        analyze("update users set text = ? where text = ? and \"_version\" >= ?",
            new Object[]{"already in panic", "don't panic", 3});
    }

    @Test
    public void testUpdateWhereVersionIsColumn() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(VERSION_EX_FROM_VALIDATOR);
        analyze("update users set col2 = ? where _version = id",
            new Object[]{1});
    }

    @Test
    public void testUpdateWhereVersionInOperatorColumn() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage(UpdateAnalyzer.VERSION_SEARCH_EX_MSG);
        analyze("update users set col2 = ? where _version in (1,2,3)",
            new Object[]{1});
    }

    @Test
    public void testUpdateWhereVersionOrOperatorColumn() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage(UpdateAnalyzer.VERSION_SEARCH_EX_MSG);
        analyze("update users set col2 = ? where _version = 1 or _version = 2",
            new Object[]{1});
    }


    @Test
    public void testUpdateWhereVersionAddition() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(VERSION_EX_FROM_VALIDATOR);
        analyze("update users set col2 = ? where _version + 1 = 2",
            new Object[]{1});
    }

    @Test
    public void testUpdateWhereVersionNotPredicate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(VERSION_EX_FROM_VALIDATOR);
        analyze("update users set text = ? where not (_version = 1 and id = 1)",
            new Object[]{"Hello"});
    }

    @Test
    public void testUpdateWhereVersionOrOperator() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage(UpdateAnalyzer.VERSION_SEARCH_EX_MSG);
        analyze("update users set awesome =  true where _version = 1 or _version = 2");
    }

    @Test
    public void testUpdateWithVersionZero() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage(UpdateAnalyzer.VERSION_SEARCH_EX_MSG);
        analyze("update users set awesome=true where name='Ford' and _version=0").nestedStatements().get(0);
    }


    @Test
    public void testSelectWhereVersionIsNullPredicate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(VERSION_EX_FROM_VALIDATOR);
        analyze("update users set col2 = ? where _version is null",
            new Object[]{1});
    }
}
