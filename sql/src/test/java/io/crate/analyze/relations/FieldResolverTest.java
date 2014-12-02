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

package io.crate.analyze.relations;

import com.google.common.collect.ImmutableMap;
import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Reference;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;

import java.util.*;

import static io.crate.testing.TestingHelpers.isReference;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FieldResolverTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Map<QualifiedName, AnalyzedRelation> dummySources = ImmutableMap.<QualifiedName, AnalyzedRelation>of(
            newQN("dummy.t"), new DummyRelation(newTI("dummy.t"), "name"));

    private static QualifiedName newQN(String dottedName) {
        return new QualifiedName(Arrays.asList(dottedName.split("\\.")));
    }

    private static TableIdent newTI(String dottedTableIdent) {
        String[] split = dottedTableIdent.split("\\.");
        return new TableIdent(split[0], split[1]);
    }

    @Test
    public void testUnknownSysTable() throws Exception {
        expectedException.expect(TableUnknownException.class);

        SysSchemaInfo sysSchemaInfo = mock(SysSchemaInfo.class);
        when(sysSchemaInfo.getTableInfo(anyString())).thenReturn(null);
        FieldResolver resolver = new FieldResolver(dummySources, sysSchemaInfo);

        resolver.resolveField(newQN("sys.foo.name"), false);
    }

    @Test
    public void testInvalidSources() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        AnalyzedRelation relation = new DummyRelation(newTI("dummy.t"), "name");
        FieldResolver resolver = new FieldResolver(
                ImmutableMap.of(newQN("too.many.parts"), relation), null);
        resolver.resolveField(newQN("name"), false);
    }

    @Test
    public void testUnknownColumnOnSysTable() throws Exception {
        expectedException.expect(ColumnUnknownException.class);

        TableInfo tableInfo = mock(TableInfo.class);
        SysSchemaInfo sysSchemaInfo = mock(SysSchemaInfo.class);
        when(sysSchemaInfo.getTableInfo(anyString())).thenReturn(tableInfo);
        when(tableInfo.getReferenceInfo(Matchers.<ColumnIdent>anyObject())).thenReturn(null);

        FieldResolver resolver = new FieldResolver(dummySources, sysSchemaInfo);
        resolver.resolveField(newQN("sys.nodes.unknown"), false);
    }

    @Test
    public void testUnknownSchema() throws Exception {
        expectedException.expect(SchemaUnknownException.class);
        FieldResolver resolver = new FieldResolver(dummySources, null);
        resolver.resolveField(newQN("invalid.table.name"), false);
    }

    @Test
    public void testUnknownTable() throws Exception {
        expectedException.expect(TableUnknownException.class);
        FieldResolver resolver = new FieldResolver(dummySources, null);
        resolver.resolveField(newQN("dummy.invalid.name"), false);
    }

    @Test
    public void testKnownSysColumn() throws Exception {
        TableInfo tableInfo = mock(TableInfo.class);
        SysSchemaInfo sysSchemaInfo = mock(SysSchemaInfo.class);
        when(sysSchemaInfo.getTableInfo(anyString())).thenReturn(tableInfo);
        when(tableInfo.getReferenceInfo(Matchers.<ColumnIdent>anyObject())).thenReturn(new ReferenceInfo(
                new ReferenceIdent(newTI("sys.nodes"), "name"), RowGranularity.NODE, DataTypes.STRING));

        FieldResolver resolver = new FieldResolver(dummySources, sysSchemaInfo);

        Field field = resolver.resolveField(newQN("sys.nodes.name"), false);
        assertThat(field.target(), isReference("name"));
        assertThat(field.relation(), instanceOf(TableRelation.class));
    }

    @Test
    public void testRegularColumnUnknown() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        FieldResolver resolver = new FieldResolver(dummySources, null);
        resolver.resolveField(newQN("age"), false);
    }

    @Test
    public void testResolveDynamicReference() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column 'age' unknown");
        AnalyzedRelation barT = new DummyRelation(newTI("bar.t"), "name");
        FieldResolver resolver = new FieldResolver(ImmutableMap.of(newQN("bar.t"), barT), null);
        resolver.resolveField(newQN("t.age"), false);
    }

    @Test
    public void testMultipleSourcesWithDynamicReferenceAndReference() throws Exception {
        AnalyzedRelation barT = new DummyRelation(newTI("bar.t"), "name");
        AnalyzedRelation fooT = new DummyRelation(newTI("foo.t"), "name");
        AnalyzedRelation fooA = new DummyRelation(newTI("foo.a"), "name");
        AnalyzedRelation customT = new DummyRelation(newTI("custom.t"), "tags");

        FieldResolver resolver = new FieldResolver(ImmutableMap.of(
                newQN("bar.t"), barT,
                newQN("foo.t"), fooT,
                newQN("foo.a"), fooA,
                newQN("custom.t"), customT), null);
        Field field = resolver.resolveField(newQN("foo.t.name"), false);
        assertThat(field.relation(), equalTo(fooT));

        // reference > dynamicReference - not ambiguous
        Field tags = resolver.resolveField(newQN("tags"), false);
        assertThat(tags.relation(), equalTo(customT));

        field = resolver.resolveField(newQN("a.name"), false);
        assertThat(field.relation(), equalTo(fooA));
    }

    @Test
    public void testRelationOutputFromAlias() throws Exception {
        // t.name from doc.foo t
        AnalyzedRelation relation = new DummyRelation(newTI("doc.foo"), "name");
        FieldResolver resolver = new FieldResolver(ImmutableMap.of(
                new QualifiedName(Arrays.asList("t")), relation),
                null);
        Field field = resolver.resolveField(newQN("t.name"), false);
        assertThat(field.relation(), equalTo(relation));
        assertThat(field.target(), isReference("name"));
    }

    @Test
    public void testRelationOutputFromSingleColumnName() throws Exception {
        // select name from t
        AnalyzedRelation relation = new DummyRelation(newTI("doc.t"), "name");
        FieldResolver resolver = new FieldResolver(ImmutableMap.of(newQN("doc.t"), relation), null);
        Field field = resolver.resolveField(newQN("name"), false);
        assertThat(field.relation(), equalTo(relation));
        assertThat(field.target(), isReference("name"));
    }

    @Test
    public void testRelationOutputFromSchemaTableColumnName() throws Exception {
        // doc.t.name from t.name

        AnalyzedRelation relation = new DummyRelation(newTI("doc.t"), "name");
        FieldResolver resolver = new FieldResolver(ImmutableMap.of(newQN("doc.t"), relation), null);
        Field field = resolver.resolveField(newQN("doc.t.name"), false);
        assertThat(field.relation(), equalTo(relation));
        assertThat(field.target(), isReference("name"));
    }

    @Test
    public void testTooManyParts() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        FieldResolver resolver = new FieldResolver(dummySources, null);
        resolver.resolveField(new QualifiedName(Arrays.asList("a", "b", "c", "d")), false);
    }

    @Test
    public void testRelationFromTwoTablesWithSameNameDifferentSchemaIsAmbiguous() throws Exception {
        // select t.name from custom.t.name, doc.t.name
        expectedException.expect(AmbiguousColumnException.class);
        expectedException.expectMessage("Column \"name\" is ambiguous");

        FieldResolver resolver = new FieldResolver(
                ImmutableMap.<QualifiedName, AnalyzedRelation>of(
                        new QualifiedName(Arrays.asList("custom", "t")), new DummyRelation(new TableIdent("custom", "t"), "name"),
                        new QualifiedName(Arrays.asList("doc", "t")), new DummyRelation(new TableIdent("doc", "t"), "name")),
                new SysSchemaInfo(mock(ClusterService.class))
        );
        resolver.resolveField(new QualifiedName(Arrays.asList("t", "name")), false);
    }


    private static class DummyRelation implements AnalyzedRelation {

        private final Set<String> supportedReference = new HashSet<>();
        private final TableIdent tableIdent;

        public DummyRelation(TableIdent tableIdent, String referenceName) {
            this.tableIdent = tableIdent;
            supportedReference.add(referenceName);
        }

        @Override
        public <C, R> R accept(RelationVisitor<C, R> visitor, C context) {
            return null;
        }

        @Override
        public Field getField(ColumnIdent columnIdent) {
            if (supportedReference.contains(columnIdent.name())) {
                return new Field(this, columnIdent.sqlFqn(), new Reference(new ReferenceInfo(
                        new ReferenceIdent(tableIdent, columnIdent), RowGranularity.DOC, DataTypes.STRING)));
            }
            return null;
        }

        @Override
        public Field getWritableField(ColumnIdent path) throws UnsupportedOperationException {
            return null;
        }

        @Override
        public List<Field> fields() {
            return null;
        }
    }

}