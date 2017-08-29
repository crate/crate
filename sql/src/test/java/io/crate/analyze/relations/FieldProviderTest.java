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
import io.crate.analyze.symbol.Field;
import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.RelationUnknownException;
import io.crate.metadata.Schemas;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.DummyRelation;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

public class FieldProviderTest extends CrateUnitTest {

    private AnalyzedRelation dummyRelation = new DummyRelation("name");

    private Map<QualifiedName, AnalyzedRelation> dummySources = ImmutableMap.of(
        newQN("dummy.t"), dummyRelation);

    private static QualifiedName newQN(String dottedName) {
        return new QualifiedName(Arrays.asList(dottedName.split("\\.")));
    }

    private static FullQualifiedNameFieldProvider newFQFieldProvider(Map<QualifiedName, AnalyzedRelation> sources) {
        return new FullQualifiedNameFieldProvider(sources, ParentRelations.NO_PARENTS, Schemas.DOC_SCHEMA_NAME);
    }

    @Test
    public void testInvalidSources() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        AnalyzedRelation relation = new DummyRelation("name");
        FieldProvider<Field> resolver = newFQFieldProvider(
            ImmutableMap.of(newQN("too.many.parts"), relation));
        resolver.resolveField(newQN("name"), Operation.READ);
    }

    @Test
    public void testUnknownSchema() throws Exception {
        expectedException.expect(RelationUnknownException.class);
        expectedException.expectMessage("Cannot resolve relation 'invalid.table'");
        FieldProvider<Field> resolver = newFQFieldProvider(dummySources);
        resolver.resolveField(newQN("invalid.table.name"), Operation.READ);
    }

    @Test
    public void testUnknownTable() throws Exception {
        expectedException.expect(RelationUnknownException.class);
        expectedException.expectMessage("Cannot resolve relation 'dummy.invalid'");
        FieldProvider<Field> resolver = newFQFieldProvider(dummySources);
        resolver.resolveField(newQN("dummy.invalid.name"), Operation.READ);
    }

    @Test
    public void testSysColumnWithoutSourceRelation() throws Exception {
        expectedException.expect(RelationUnknownException.class);
        expectedException.expectMessage("Cannot resolve relation 'sys.nodes'");
        FieldProvider<Field> resolver = newFQFieldProvider(dummySources);

        resolver.resolveField(newQN("sys.nodes.name"), Operation.READ);
    }

    @Test
    public void testRegularColumnUnknown() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        FieldProvider<Field> resolver = newFQFieldProvider(dummySources);
        resolver.resolveField(newQN("age"), Operation.READ);
    }

    @Test
    public void testResolveDynamicReference() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column age unknown");
        AnalyzedRelation barT = new DummyRelation("name");
        FieldProvider<Field> resolver = newFQFieldProvider(ImmutableMap.of(newQN("bar.t"), barT));
        resolver.resolveField(newQN("t.age"), Operation.READ);
    }

    @Test
    public void testMultipleSourcesWithDynamicReferenceAndReference() throws Exception {
        AnalyzedRelation barT = new DummyRelation("name");
        AnalyzedRelation fooT = new DummyRelation("name");
        AnalyzedRelation fooA = new DummyRelation("name");
        AnalyzedRelation customT = new DummyRelation("tags");

        FieldProvider<Field> resolver = newFQFieldProvider(ImmutableMap.of(
            newQN("bar.t"), barT,
            newQN("foo.t"), fooT,
            newQN("foo.a"), fooA,
            newQN("custom.t"), customT));
        Field field = resolver.resolveField(newQN("foo.t.name"), Operation.READ);
        assertThat(field.relation(), equalTo(fooT));

        // reference > dynamicReference - not ambiguous
        Field tags = resolver.resolveField(newQN("tags"), Operation.READ);
        assertThat(tags.relation(), equalTo(customT));

        field = resolver.resolveField(newQN("a.name"), Operation.READ);
        assertThat(field.relation(), equalTo(fooA));
    }

    @Test
    public void testRelationOutputFromAlias() throws Exception {
        // t.name from doc.foo t
        AnalyzedRelation relation = new DummyRelation("name");
        FieldProvider<Field> resolver = newFQFieldProvider(ImmutableMap.of(
            new QualifiedName(Arrays.asList("t")), relation));
        Field field = resolver.resolveField(newQN("t.name"), Operation.READ);
        assertThat(field.relation(), equalTo(relation));
        assertThat(field.path().outputName(), is("name"));
    }

    @Test
    public void testRelationOutputFromSingleColumnName() throws Exception {
        // select name from t
        AnalyzedRelation relation = new DummyRelation("name");
        FieldProvider<Field> resolver = newFQFieldProvider(ImmutableMap.of(newQN("doc.t"), relation));
        Field field = resolver.resolveField(newQN("name"), Operation.READ);
        assertThat(field.relation(), equalTo(relation));
        assertThat(field.path().outputName(), is("name"));
    }

    @Test
    public void testRelationOutputFromSchemaTableColumnName() throws Exception {
        // doc.t.name from t.name

        AnalyzedRelation relation = new DummyRelation("name");
        FieldProvider<Field> resolver = newFQFieldProvider(ImmutableMap.of(newQN("doc.t"), relation));
        Field field = resolver.resolveField(newQN("doc.t.name"), Operation.INSERT);
        assertThat(field.relation(), equalTo(relation));
        assertThat(field.path().outputName(), is("name"));
    }

    @Test
    public void testTooManyParts() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        FieldProvider<Field> resolver = newFQFieldProvider(dummySources);
        resolver.resolveField(new QualifiedName(Arrays.asList("a", "b", "c", "d")), Operation.READ);
    }

    @Test
    public void testTooManyPartsNameFieldResolver() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Column reference \"a.b\" has too many parts. A column must not have a schema or a table here.");
        FieldProvider<Field> resolver = new NameFieldProvider(dummyRelation);
        resolver.resolveField(new QualifiedName(Arrays.asList("a", "b")), Operation.READ);
    }

    @Test
    public void testRelationFromTwoTablesWithSameNameDifferentSchemaIsAmbiguous() throws Exception {
        // select t.name from custom.t.name, doc.t.name
        expectedException.expect(AmbiguousColumnException.class);
        expectedException.expectMessage("Column \"name\" is ambiguous");

        FieldProvider<Field> resolver = newFQFieldProvider(
            ImmutableMap.<QualifiedName, AnalyzedRelation>of(
                new QualifiedName(Arrays.asList("custom", "t")), new DummyRelation("name"),
                new QualifiedName(Arrays.asList("doc", "t")), new DummyRelation("name"))
        );
        resolver.resolveField(new QualifiedName(Arrays.asList("t", "name")), Operation.READ);
    }

    @Test
    public void testRelationFromTwoTables() throws Exception {
        // select name from doc.t, custom.t
        FieldProvider<Field> resolver = newFQFieldProvider(
            ImmutableMap.<QualifiedName, AnalyzedRelation>of(
                new QualifiedName(Arrays.asList("custom", "t")), new DummyRelation("address"),
                new QualifiedName(Arrays.asList("doc", "t")), new DummyRelation("name"))
        );
        resolver.resolveField(new QualifiedName(Arrays.asList("t", "name")), Operation.READ);
    }

    @Test
    public void testSimpleFieldResolver() throws Exception {
        // select name from doc.t
        AnalyzedRelation relation = new DummyRelation("name");
        FieldProvider<Field> resolver = new NameFieldProvider(relation);
        Field field = resolver.resolveField(new QualifiedName(Arrays.asList("name")), Operation.READ);
        assertThat(field.relation(), equalTo(relation));
    }

    @Test
    public void testSimpleResolverUnknownColumn() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column unknown unknown");
        AnalyzedRelation relation = new DummyRelation("name");
        FieldProvider<Field> resolver = newFQFieldProvider(ImmutableMap.of(newQN("doc.t"), relation));
        resolver.resolveField(new QualifiedName(Arrays.asList("unknown")), Operation.READ);
    }

    @Test
    public void testColumnSchemaResolver() throws Exception {
        AnalyzedRelation barT = new DummyRelation("\"Name\"");

        FieldProvider<Field> resolver = newFQFieldProvider(ImmutableMap.of(newQN("\"Bar\""), barT));
        Field field = resolver.resolveField(newQN("\"Foo\".\"Bar\".\"Name\""), Operation.READ);
        assertThat(field.relation(), equalTo(barT));
    }

    @Test
    public void testColumnSchemaResolverFail() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column name unknown");
        AnalyzedRelation barT = new DummyRelation("\"Name\"");
        FieldProvider<Field> resolver = newFQFieldProvider(ImmutableMap.of(newQN("bar"), barT));
        resolver.resolveField(newQN("bar.name"), Operation.READ);
    }

    @Test
    public void testAliasRelationNameResolver() throws Exception {
        AnalyzedRelation barT = new DummyRelation("name");

        FieldProvider<Field> resolver = newFQFieldProvider(ImmutableMap.of(newQN("\"Bar\""), barT));
        Field field = resolver.resolveField(newQN("\"Bar\".name"), Operation.READ);
        assertThat(field.relation(), equalTo(barT));
    }

    @Test
    public void testAliasRelationNameResolverFail() throws Exception {
        expectedException.expect(RelationUnknownException.class);
        expectedException.expectMessage("Cannot resolve relation 'doc.\"Bar\"'");
        AnalyzedRelation barT = new DummyRelation("name");
        FieldProvider<Field> resolver = newFQFieldProvider(ImmutableMap.of(newQN("bar"), barT));
        resolver.resolveField(newQN("\"Bar\".name"), Operation.READ);
    }
}
