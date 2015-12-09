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
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Path;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

public class FieldProviderTest extends CrateUnitTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private AnalyzedRelation dummyRelation = new DummyRelation("name");

    private Map<QualifiedName, AnalyzedRelation> dummySources = ImmutableMap.of(
            newQN("dummy.t"), dummyRelation);

    private static QualifiedName newQN(String dottedName) {
        return new QualifiedName(Arrays.asList(dottedName.split("\\.")));
    }

    @Test
    public void testInvalidSources() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        AnalyzedRelation relation = new DummyRelation("name");
        FieldProvider<Field>resolver = new FullQualifedNameFieldProvider(
                ImmutableMap.of(newQN("too.many.parts"), relation));
        resolver.resolveField(newQN("name"), false);
    }

    @Test
    public void testUnknownSchema() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot resolve relation 'invalid.table'");
        FieldProvider<Field>resolver = new FullQualifedNameFieldProvider(dummySources);
        resolver.resolveField(newQN("invalid.table.name"), false);
    }

    @Test
    public void testUnknownTable() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot resolve relation 'invalid'");
        FieldProvider<Field>resolver = new FullQualifedNameFieldProvider(dummySources);
        resolver.resolveField(newQN("dummy.invalid.name"), false);
    }

    @Test
    public void testSysColumnWithoutSourceRelation() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot resolve relation 'sys.nodes'");
        FieldProvider<Field>resolver = new FullQualifedNameFieldProvider(dummySources);

        resolver.resolveField(newQN("sys.nodes.name"), false);
    }

    @Test
    public void testRegularColumnUnknown() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        FieldProvider<Field>resolver = new FullQualifedNameFieldProvider(dummySources);
        resolver.resolveField(newQN("age"), false);
    }

    @Test
    public void testResolveDynamicReference() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column age unknown");
        AnalyzedRelation barT = new DummyRelation("name");
        FieldProvider<Field>resolver = new FullQualifedNameFieldProvider(ImmutableMap.of(newQN("bar.t"), barT));
        resolver.resolveField(newQN("t.age"), false);
    }

    @Test
    public void testMultipleSourcesWithDynamicReferenceAndReference() throws Exception {
        AnalyzedRelation barT = new DummyRelation("name");
        AnalyzedRelation fooT = new DummyRelation("name");
        AnalyzedRelation fooA = new DummyRelation("name");
        AnalyzedRelation customT = new DummyRelation("tags");

        FieldProvider<Field> resolver = new FullQualifedNameFieldProvider(ImmutableMap.of(
                newQN("bar.t"), barT,
                newQN("foo.t"), fooT,
                newQN("foo.a"), fooA,
                newQN("custom.t"), customT));
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
        AnalyzedRelation relation = new DummyRelation("name");
        FieldProvider<Field> resolver = new FullQualifedNameFieldProvider(ImmutableMap.of(
                new QualifiedName(Arrays.asList("t")), relation));
        Field field = resolver.resolveField(newQN("t.name"), false);
        assertThat(field.relation(), equalTo(relation));
        assertThat(field.path().outputName(), is("name"));
    }

    @Test
    public void testRelationOutputFromSingleColumnName() throws Exception {
        // select name from t
        AnalyzedRelation relation = new DummyRelation( "name");
        FieldProvider<Field>resolver = new FullQualifedNameFieldProvider(ImmutableMap.of(newQN("doc.t"), relation));
        Field field = resolver.resolveField(newQN("name"), false);
        assertThat(field.relation(), equalTo(relation));
        assertThat(field.path().outputName(), is("name"));
    }

    @Test
    public void testRelationOutputFromSchemaTableColumnName() throws Exception {
        // doc.t.name from t.name

        AnalyzedRelation relation = new DummyRelation("name");
        FieldProvider<Field>resolver = new FullQualifedNameFieldProvider(ImmutableMap.of(newQN("doc.t"), relation));
        Field field = resolver.resolveField(newQN("doc.t.name"), true);
        assertThat(field.relation(), equalTo(relation));
        assertThat(field.path().outputName(), is("name"));
    }

    @Test
    public void testTooManyParts() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        FieldProvider<Field>resolver = new FullQualifedNameFieldProvider(dummySources);
        resolver.resolveField(new QualifiedName(Arrays.asList("a", "b", "c", "d")), false);
    }

    @Test
    public void testTooManyPartsNameFieldResolver() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Column reference \"a.b\" has too many parts. A column must not have a schema or a table here.");
        FieldProvider<Field>resolver = new NameFieldProvider(dummyRelation);
        resolver.resolveField(new QualifiedName(Arrays.asList("a", "b")), false);
    }

    @Test
    public void testRelationFromTwoTablesWithSameNameDifferentSchemaIsAmbiguous() throws Exception {
        // select t.name from custom.t.name, doc.t.name
        expectedException.expect(AmbiguousColumnException.class);
        expectedException.expectMessage("Column \"name\" is ambiguous");

        FieldProvider<Field>resolver = new FullQualifedNameFieldProvider(
                ImmutableMap.<QualifiedName, AnalyzedRelation>of(
                        new QualifiedName(Arrays.asList("custom", "t")), new DummyRelation("name"),
                        new QualifiedName(Arrays.asList("doc", "t")), new DummyRelation("name"))
        );
        resolver.resolveField(new QualifiedName(Arrays.asList("t", "name")), false);
    }

    @Test
    public void testRelationFromTwoTables() throws Exception {
        // select name from doc.t, custom.t
        FieldProvider<Field>resolver = new FullQualifedNameFieldProvider(
                ImmutableMap.<QualifiedName, AnalyzedRelation>of(
                        new QualifiedName(Arrays.asList("custom", "t")), new DummyRelation("address"),
                        new QualifiedName(Arrays.asList("doc", "t")), new DummyRelation("name"))
        );
        resolver.resolveField(new QualifiedName(Arrays.asList("t", "name")), false);
    }

    @Test
    public void testSimpleFieldResolver() throws Exception {
        // select name from doc.t
        AnalyzedRelation relation = new DummyRelation("name");
        FieldProvider<Field>resolver = new NameFieldProvider(relation);
        Field field = resolver.resolveField(new QualifiedName(Arrays.asList("name")), false);
        assertThat(field.relation(), equalTo(relation));
    }

    @Test
    public void testSimpleResolverUnknownColumn() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column unknown unknown");
        AnalyzedRelation relation = new DummyRelation("name");
        FieldProvider<Field>resolver = new FullQualifedNameFieldProvider(ImmutableMap.of(newQN("doc.t"), relation));
        resolver.resolveField(new QualifiedName(Arrays.asList("unknown")), false);
    }

    private static class DummyRelation implements AnalyzedRelation {

        private final Set<String> supportedReference = new HashSet<>();

        public DummyRelation( String referenceName) {
            supportedReference.add(referenceName);
        }

        @Override
        public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
            return null;
        }

        @Override
        public Field getField(Path path) {
            ColumnIdent columnIdent = (ColumnIdent) path;
            if (supportedReference.contains(columnIdent.name())) {
                return new Field(this, columnIdent, DataTypes.STRING);
            }
            return null;
        }

        @Override
        public Field getWritableField(Path path) throws UnsupportedOperationException {
            return getField(path);
        }

        @Override
        public List<Field> fields() {
            return null;
        }
    }

}