/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.analyze.relations;

import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.RelationUnknown;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.test.ESTestCase;
import io.crate.testing.DummyRelation;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static io.crate.testing.SymbolMatchers.isField;

public class FieldProviderTest extends ESTestCase {

    private AnalyzedRelation dummyRelation = new DummyRelation("name");

    private Map<QualifiedName, AnalyzedRelation> dummySources = Map.of(new QualifiedName("dummy"), dummyRelation);

    private static QualifiedName newQN(String dottedName) {
        return new QualifiedName(Arrays.asList(dottedName.split("\\.")));
    }

    private static FullQualifiedNameFieldProvider newFQFieldProvider(Map<QualifiedName, AnalyzedRelation> sources) {
        Map<RelationName, AnalyzedRelation> relations = sources.entrySet().stream()
            .collect(Collectors.toMap(
                entry -> RelationName.of(entry.getKey(), "doc"),
                Map.Entry::getValue
            ));
        return new FullQualifiedNameFieldProvider(
            relations,
            ParentRelations.NO_PARENTS,
            Schemas.DOC_SCHEMA_NAME
        );
    }

    @Test
    public void testInvalidSources() throws Exception {
        expectedException.expectMessage("Table with more than 2 QualifiedName parts is not supported. Only <schema>.<tableName> works.");
        AnalyzedRelation relation = new DummyRelation("name");
        FieldProvider<Symbol> resolver = newFQFieldProvider(Map.of(newQN("too.many.parts"), relation));
        resolver.resolveField(newQN("name"), null, Operation.READ);
    }

    @Test
    public void testUnknownSchema() throws Exception {
        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage("Relation 'invalid.table' unknown");
        FieldProvider<Symbol> resolver = newFQFieldProvider(dummySources);
        resolver.resolveField(newQN("invalid.table.name"), null, Operation.READ);
    }

    @Test
    public void testUnknownTable() throws Exception {
        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage("Relation 'dummy.invalid' unknown");
        FieldProvider<Symbol> resolver = newFQFieldProvider(dummySources);
        resolver.resolveField(newQN("dummy.invalid.name"), null, Operation.READ);
    }

    @Test
    public void testSysColumnWithoutSourceRelation() throws Exception {
        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage("Relation 'sys.nodes' unknown");
        FieldProvider<Symbol> resolver = newFQFieldProvider(dummySources);

        resolver.resolveField(newQN("sys.nodes.name"), null, Operation.READ);
    }

    @Test
    public void testRegularColumnUnknown() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        FieldProvider<Symbol> resolver = newFQFieldProvider(dummySources);
        resolver.resolveField(newQN("age"), null, Operation.READ);
    }

    @Test
    public void testResolveDynamicReference() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column age unknown");
        AnalyzedRelation barT = new DummyRelation("name");
        FieldProvider<Symbol> resolver = newFQFieldProvider(Map.of(newQN("bar.t"), barT));
        resolver.resolveField(newQN("t.age"), null, Operation.READ);
    }

    @Test
    public void testMultipleSourcesWithDynamicReferenceAndReference() throws Exception {
        AnalyzedRelation barT = new DummyRelation(new RelationName("bar", "t"), "name");
        AnalyzedRelation fooT = new DummyRelation(new RelationName("foo", "t"), "name");
        AnalyzedRelation fooA = new DummyRelation(new RelationName("foo", "a"), "name");
        AnalyzedRelation customT = new DummyRelation(new RelationName("custom", "t"), "tags");

        FieldProvider<Symbol> resolver = newFQFieldProvider(Map.of(
            newQN("bar.t"), barT,
            newQN("foo.t"), fooT,
            newQN("foo.a"), fooA,
            newQN("custom.t"), customT));
        Symbol field = resolver.resolveField(newQN("foo.t.name"), null, Operation.READ);
        assertThat(field, isField("name", fooT.relationName()));

        Symbol tags = resolver.resolveField(newQN("tags"), null, Operation.READ);
        assertThat(tags, isField("tags", customT.relationName()));

        field = resolver.resolveField(newQN("a.name"), null, Operation.READ);
        assertThat(field, isField("name", fooA.relationName()));
    }

    @Test
    public void testRelationOutputFromAlias() throws Exception {
        // t.name from doc.foo t
        AnalyzedRelation relation = new DummyRelation(new RelationName("doc", "t"), "name");
        FieldProvider<Symbol> resolver = newFQFieldProvider(Map.of(
            new QualifiedName(Arrays.asList("t")), relation));
        Symbol field = resolver.resolveField(newQN("t.name"), null, Operation.READ);
        assertThat(field, isField("name", relation.relationName()));
    }

    @Test
    public void testRelationOutputFromSingleColumnName() throws Exception {
        // select name from t
        AnalyzedRelation relation = new DummyRelation("name");
        FieldProvider<Symbol> resolver = newFQFieldProvider(Map.of(newQN("doc.t"), relation));
        Symbol field = resolver.resolveField(newQN("name"), null, Operation.READ);
        assertThat(field, isField("name", relation.relationName()));
    }

    @Test
    public void testRelationOutputFromSchemaTableColumnName() throws Exception {
        // doc.t.name from t.name

        AnalyzedRelation relation = new DummyRelation(new RelationName("doc", "t"), "name");
        FieldProvider<Symbol> resolver = newFQFieldProvider(Map.of(newQN("doc.t"), relation));
        Symbol field = resolver.resolveField(newQN("doc.t.name"), null, Operation.INSERT);
        assertThat(field, isField("name", relation.relationName()));
    }

    @Test
    public void testTooManyParts() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        FieldProvider<Symbol> resolver = newFQFieldProvider(dummySources);
        resolver.resolveField(new QualifiedName(Arrays.asList("a", "b", "c", "d")), null, Operation.READ);
    }

    @Test
    public void testTooManyPartsNameFieldResolver() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Column reference \"a.b\" has too many parts. A column must not have a schema or a table here.");
        FieldProvider<Symbol> resolver = new NameFieldProvider(dummyRelation);
        resolver.resolveField(new QualifiedName(Arrays.asList("a", "b")), null, Operation.READ);
    }

    @Test
    public void testRelationFromTwoTablesWithSameNameDifferentSchemaIsAmbiguous() throws Exception {
        // select t.name from custom.t.name, doc.t.name
        expectedException.expect(AmbiguousColumnException.class);
        expectedException.expectMessage("Column \"name\" is ambiguous");

        FieldProvider<Symbol> resolver = newFQFieldProvider(
            Map.of(
                new QualifiedName(Arrays.asList("custom", "t")), new DummyRelation("name"),
                new QualifiedName(Arrays.asList("doc", "t")), new DummyRelation("name"))
        );
        resolver.resolveField(new QualifiedName(Arrays.asList("t", "name")), null, Operation.READ);
    }

    @Test
    public void testRelationFromTwoTables() throws Exception {
        // select name from doc.t, custom.t
        FieldProvider<Symbol> resolver = newFQFieldProvider(
            Map.of(
                new QualifiedName(Arrays.asList("custom", "t")), new DummyRelation("address"),
                new QualifiedName(Arrays.asList("doc", "t")), new DummyRelation("name"))
        );
        resolver.resolveField(new QualifiedName(Arrays.asList("t", "name")), null, Operation.READ);
    }

    @Test
    public void testSimpleFieldResolver() throws Exception {
        // select name from doc.t
        AnalyzedRelation relation = new DummyRelation("name");
        FieldProvider<Symbol> resolver = new NameFieldProvider(relation);
        Symbol field = resolver.resolveField(new QualifiedName(Arrays.asList("name")), null, Operation.READ);
        assertThat(field, isField("name", relation.relationName()));
    }

    @Test
    public void testSimpleResolverUnknownColumn() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column unknown unknown");
        AnalyzedRelation relation = new DummyRelation("name");
        FieldProvider<Symbol> resolver = newFQFieldProvider(Map.of(newQN("doc.t"), relation));
        resolver.resolveField(new QualifiedName(Arrays.asList("unknown")), null, Operation.READ);
    }

    @Test
    public void testColumnSchemaResolver() throws Exception {
        AnalyzedRelation barT = new DummyRelation(new RelationName("Foo", "Bar"), "\"Name\"");

        FieldProvider<Symbol> resolver = newFQFieldProvider(Map.of(newQN("\"Foo\".\"Bar\""), barT));
        Symbol field = resolver.resolveField(newQN("\"Foo\".\"Bar\".\"Name\""), null, Operation.READ);
        assertThat(field, isField("\"Name\"", barT.relationName()));
    }

    @Test
    public void testColumnSchemaResolverFail() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column name unknown");
        AnalyzedRelation barT = new DummyRelation("\"Name\"");
        FieldProvider<Symbol> resolver = newFQFieldProvider(Map.of(newQN("bar"), barT));
        resolver.resolveField(newQN("bar.name"), null, Operation.READ);
    }

    @Test
    public void testAliasRelationNameResolver() throws Exception {
        AnalyzedRelation barT = new DummyRelation(new RelationName("doc", "Bar"), "name");

        FieldProvider<Symbol> resolver = newFQFieldProvider(Map.of(newQN("\"Bar\""), barT));
        Symbol field = resolver.resolveField(newQN("\"Bar\".name"), null, Operation.READ);
        assertThat(field, isField("name", barT.relationName()));
    }

    @Test
    public void testAliasRelationNameResolverFail() throws Exception {
        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage("Relation 'doc.\"Bar\"' unknown");
        AnalyzedRelation barT = new DummyRelation("name");
        FieldProvider<Symbol> resolver = newFQFieldProvider(Map.of(newQN("bar"), barT));
        resolver.resolveField(newQN("\"Bar\".name"), null, Operation.READ);
    }
}
