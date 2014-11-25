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

import com.google.common.collect.ImmutableList;
import io.crate.PartitionName;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import io.crate.sql.parser.SqlParser;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import junit.framework.Assert;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Module;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;
import static junit.framework.Assert.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UpdateAnalyzerTest extends BaseAnalyzerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final static TableIdent NESTED_CLUSTERED_BY_TABLE_IDENT = new TableIdent("doc", "nestedclustered");
    private final static TableInfo NESTED_CLUSTERED_BY_TABLE_INFO = TestingTableInfo.builder(
            NESTED_CLUSTERED_BY_TABLE_IDENT, RowGranularity.DOC, shardRouting)
            .add("obj", DataTypes.OBJECT, null)
            .add("obj", DataTypes.STRING, Arrays.asList("name"))
            .clusteredBy("obj.name").build();

    private final static TableIdent TEST_ALIAS_TABLE_IDENT = new TableIdent(null, "alias");
    private final static TableInfo TEST_ALIAS_TABLE_INFO = new TestingTableInfo.Builder(
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
            when(schemaInfo.getTableInfo(TEST_DOC_TABLE_IDENT_CLUSTERED_BY_ONLY.name())).thenReturn(userTableInfoClusteredByOnly);
            when(schemaInfo.getTableInfo(TEST_PARTITIONED_TABLE_IDENT.name()))
                    .thenReturn(TEST_PARTITIONED_TABLE_INFO);
            when(schemaInfo.getTableInfo(DEEPLY_NESTED_TABLE_IDENT.name())).thenReturn(DEEPLY_NESTED_TABLE_INFO);
            when(schemaInfo.getTableInfo(NESTED_CLUSTERED_BY_TABLE_IDENT.name())).thenReturn(NESTED_CLUSTERED_BY_TABLE_INFO);
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
                new OperatorModule(),
                new PredicateModule(),
                new MetaDataSysModule()
        ));
        return modules;
    }

    protected UpdateAnalyzedStatement.NestedAnalyzedStatement analyze(String statement) {
        return ((UpdateAnalyzedStatement) analyzer.analyze(SqlParser.createStatement(statement)).analyzedStatement())
                .nestedAnalysisList.get(0);
    }

    protected UpdateAnalyzedStatement.NestedAnalyzedStatement analyze(String statement, Object[] params) {
        return ((UpdateAnalyzedStatement) analyzer.analyze(
                SqlParser.createStatement(statement),
                params,
                new Object[0][]).analyzedStatement()).nestedAnalysisList.get(0);
    }

    protected UpdateAnalyzedStatement analyze(String statement, Object[][] bulkArgs) {
        return (UpdateAnalyzedStatement) analyzer.analyze(SqlParser.createStatement(statement),
                new Object[0],
                bulkArgs).analyzedStatement();
    }

    @Test
    public void testUpdateAnalysis() throws Exception {
        AnalyzedStatement analyzedStatement = analyze("update users set name='Ford Prefect'");
        assertThat(analyzedStatement, instanceOf(UpdateAnalyzedStatement.NestedAnalyzedStatement.class));
    }

    @Test( expected = TableUnknownException.class)
    public void testUpdateUnknownTable() throws Exception {
        analyze("update unknown set name='Prosser'");
    }

    @Test( expected = ColumnValidationException.class)
    public void testUpdateSetColumnToColumnValue() throws Exception {
        analyze("update users set name=name");
    }

    @Test( expected = IllegalArgumentException.class )
    public void testUpdateSameReferenceRepeated() throws Exception {
        analyze("update users set name='Trillian', name='Ford'");
    }

    @Test( expected = IllegalArgumentException.class )
    public void testUpdateSameNestedReferenceRepeated() throws Exception {
        analyze("update users set details['arms']=3, details['arms']=5");
    }

    @Test( expected = UnsupportedOperationException.class )
    public void testUpdateSysTables() throws Exception {
        analyze("update sys.nodes set fs=?", new Object[]{new HashMap<String, Object>(){{
            put("free", 0);
        }}});
    }

    @Test
    public void testNumericTypeOutOfRange() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for shorts: short value out of range: -100000");

        analyze("update users set shorts=-100000");
    }


    @Test
    public void testNumericOutOfRangeFromFunction() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for bytes: byte value out of range: 1234");

        analyze("update users set bytes=abs(-1234)");
    }

    @Test
    public void testUpdateAssignments() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement analysis = analyze("update users set name='Trillian'");
        assertThat(analysis.assignments().size(), is(1));
        assertThat(analysis.table().ident(), is(new TableIdent(null, "users")));

        Reference ref = analysis.assignments().keySet().iterator().next();
        assertThat(ref.info().ident().tableIdent().name(), is("users"));
        assertThat(ref.info().ident().columnIdent().name(), is("name"));
        assertTrue(analysis.assignments().containsKey(ref));

        Symbol value = analysis.assignments().entrySet().iterator().next().getValue();
        assertLiteralSymbol(value, "Trillian");
    }

    @Test
    public void testUpdateAssignmentNestedDynamicColumn() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement analysis = analyze("update users set details['arms']=3");
        assertThat(analysis.assignments().size(), is(1));

        Reference ref = analysis.assignments().keySet().iterator().next();
        assertThat(ref, instanceOf(DynamicReference.class));
        Assert.assertEquals(DataTypes.LONG, ref.info().type());
        assertThat(ref.info().ident().columnIdent().isColumn(), is(false));
        assertThat(ref.info().ident().columnIdent().fqn(), is("details.arms"));
    }

    @Test( expected = ColumnValidationException.class)
    public void testUpdateAssignmentWrongType() throws Exception {
        analyze("update users set other_id='String'");
    }

    @Test
    public void testUpdateAssignmentConvertableType() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement analysis = analyze("update users set other_id=9.9");
        Reference ref = analysis.assignments().keySet().iterator().next();
        assertThat(ref, not(instanceOf(DynamicReference.class)));
        assertEquals(DataTypes.LONG, ref.info().type());

        Symbol value = analysis.assignments().entrySet().iterator().next().getValue();
        assertLiteralSymbol(value, 9L);
    }

    @Test
    public void testUpdateMuchAssignments() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement analysis = analyze(
                "update users set other_id=9.9, name='Trillian', details=?, stuff=true, foo='bar'",
                new Object[]{new HashMap<String, Object>()});
        assertThat(analysis.assignments().size(), is(5));
    }

    @Test
    public void testNoWhereClause() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement analysis = analyze("update users set other_id=9");
        assertThat(analysis.whereClause(), is(WhereClause.MATCH_ALL));
    }

    @Test
    public void testNoMatchWhereClause() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement analysis = analyze("update users set other_id=9 where true=false");
        assertThat(analysis.whereClause().noMatch(), is(true));
    }

    @Test
    public void testUpdateWhereClause() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement analysis =
                analyze("update users set other_id=9 where name='Trillian'");
        assertThat(analysis.whereClause().hasQuery(), is(true));
        assertThat(analysis.whereClause().noMatch(), is(false));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWrongQualifiedNameReferenceWithSchema() throws Exception {
        analyze("update users set sys.nodes.fs=?", new Object[]{new HashMap<String, Object>()});
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWrongQualifiedNameReference() throws Exception {
        analyze("update users set unknown.name='Trillian'");
    }

    @Test
    public void testQualifiedNameReference() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement analysis = analyze("update users set users.name='Trillian'");
        Reference ref = analysis.assignments().keySet().iterator().next();
        assertThat(ref.info().ident().tableIdent().name(), is("users"));
        assertThat(ref.info().ident().columnIdent().fqn(), is("name"));
    }

    @Test
    public void testUpdateWithParameter() throws Exception {
        Map[] friends = new Map[]{
                new HashMap<String, Object>() {{ put("name", "Slartibartfast"); }},
                new HashMap<String, Object>() {{ put("name", "Marvin"); }}
        };
        UpdateAnalyzedStatement.NestedAnalyzedStatement analysis =
                analyze("update users set name=?, other_id=?, friends=? where id=?",
                        new Object[]{"Jeltz", 0, friends, "9"});
        assertThat(analysis.assignments().size(), is(3));
        assertLiteralSymbol(
                analysis.assignments().get(new Reference(userTableInfo.getReferenceInfo(new ColumnIdent("name")))),
                "Jeltz"
        );
        assertLiteralSymbol(
                analysis.assignments().get(new Reference(userTableInfo.getReferenceInfo(new ColumnIdent("friends")))),
                friends,
                new ArrayType(DataTypes.OBJECT)
        );
        assertLiteralSymbol(
                analysis.assignments().get(new Reference(userTableInfo.getReferenceInfo(new ColumnIdent("other_id")))),
                0L
        );

        assertLiteralSymbol(((Function)analysis.whereClause().query()).arguments().get(1), 9L);
    }


    @Test
    public void testUpdateWithWrongParameters() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for name: cannot cast {} to string");

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
                new Object[]{ new Map[0], 0 });

        Literal friendsLiteral = (Literal)analysis.assignments().get(
                new Reference(userTableInfo.getReferenceInfo(new ColumnIdent("friends"))));
        assertThat(friendsLiteral.valueType().id(), is(ArrayType.ID));
        assertEquals(DataTypes.OBJECT, ((ArrayType)friendsLiteral.valueType()).innerType());
        assertThat(((Object[])friendsLiteral.value()).length, is(0));
    }

    @Test( expected = IllegalArgumentException.class )
    public void testUpdateSystemColumn() throws Exception {
        analyze("update users set _id=1");
    }

    @Test( expected = IllegalArgumentException.class )
    public void testUpdatePrimaryKey() throws Exception {
        analyze("update users set id=1");
    }

    @Test( expected = IllegalArgumentException.class )
    public void testUpdateClusteredBy() throws Exception {
        analyze("update users_clustered_by_only set id=1");
    }

    @Test( expected = UnsupportedOperationException.class )
    public void testUpdateWhereSysColumn() throws Exception {
        analyze("update users set name='Ford' where sys.nodes.id = 'node_1'");
    }

    @Test( expected = IllegalArgumentException.class )
    public void testUpdatePartitionedByColumn() throws Exception {
        analyze("update parted set date = 1395874800000");
    }

    @Test
    public void testUpdateWherePartitionedByColumn() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement analysis = analyze(
                "update parted set id = 2 where date = 1395874800000");
        assertThat(analysis.whereClause().hasQuery(), is(false));
        assertThat(analysis.whereClause().noMatch(), is(false));
        assertEquals(ImmutableList.of(
                        new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).stringValue()),
                analysis.whereClause().partitions()
        );
    }

    @Test
    public void testUpdateTableAlias() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement expectedAnalysis = analyze(
                "update users set awesome=true where awesome=false");
        UpdateAnalyzedStatement.NestedAnalyzedStatement actualAnalysis = analyze(
                "update users as u set u.awesome=true where u.awesome=false");

        assertEquals(actualAnalysis.tableAlias(), "u");
        assertEquals(
                expectedAnalysis.assignments(),
                actualAnalysis.assignments()
        );
        assertEquals(
                ((Function) expectedAnalysis.whereClause().query()).arguments().get(0),
                ((Function) actualAnalysis.whereClause().query()).arguments().get(0)
        );
        System.out.println();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateObjectArrayField() throws Exception {
        analyze("update users set friends['id'] = ?", new Object[]{
                new Long[]{1L, 2L, 3L}
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWhereClauseObjectArrayField() throws Exception {
        analyze("update users set awesome=true where friends['id'] = 5");
    }

    @Test
    public void testUpdateWithVersionZero() throws Exception {
        UpdateAnalyzedStatement.NestedAnalyzedStatement analysis = analyze(
                "update users set awesome=true where name='Ford' and _version=0");
        assertThat(analysis.whereClause().noMatch(), is(true));
    }

    @Test
    public void testUpdateDynamicInvalidTypeLiteral() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for new: Invalid datatype 'double_array_array'");
        analyze("update users set new=[[1.9, 4.8], [9.7, 12.7]]");
    }

    @Test
    public void testUpdateDynamicInvalidType() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for new: Invalid datatype 'double_array_array'");
        analyze("update users set new=? where id=1", new Object[]{
                new Object[] {
                        new Object[] {
                                1.9, 4.8
                        },
                        new Object[] {
                                9.7, 12.7
                        }
                }
        });
    }

    @Test
    public void testUpdateInvalidType() throws Exception {
        expectedException.expect(ColumnValidationException.class);
        expectedException.expectMessage("Validation failed for tags: Invalid datatype 'string_array_array'");
        analyze("update users set tags=? where id=1", new Object[]{
                new Object[] {
                        new Object[]{ "a", "b" }
                }
        });
    }

    @Test
    public void testUpdateNestedClusteredByColumn() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Updating a clustered-by column is not supported");
        analyze("update nestedclustered set obj = {name='foobar'}");
    }
}
