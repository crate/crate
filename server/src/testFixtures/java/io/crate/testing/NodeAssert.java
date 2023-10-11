/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.testing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Consumer;

import org.assertj.core.api.AbstractAssert;

import io.crate.sql.tree.CollectionColumnType;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.ObjectColumnType;

public final class NodeAssert extends AbstractAssert<NodeAssert, Node> {

    public NodeAssert(Node actual) {
        super(actual, NodeAssert.class);
    }

    public NodeAssert isColumnType(String expectedTypeName) {
        assertColumnType(expectedTypeName);
        isExactlyInstanceOf(ColumnType.class);
        assertThat(((ColumnType<?>) actual).parameters()).isEmpty();
        return this;
    }

    public NodeAssert isColumnType(String expectedTypeName, Integer... expectedParams) {
        assertColumnType(expectedTypeName);
        isExactlyInstanceOf(ColumnType.class);
        assertThat(((ColumnType<?>) actual).parameters()).containsExactly(expectedParams);
        return this;
    }

    public NodeAssert isCollectionColumnType(String expectedTypeName, Consumer<Node> innerTypeMatcher) {
        assertColumnType(expectedTypeName);
        isExactlyInstanceOf(CollectionColumnType.class);
        CollectionColumnType<?> collectionColumnType = (CollectionColumnType<?>) actual;

        assertThat(collectionColumnType.parameters()).isEmpty();
        assertThat(collectionColumnType.innerType()).satisfies(innerTypeMatcher);
        return this;
    }

    public NodeAssert isObjectColumnType(String expectedTypeName, Consumer<ColumnPolicy> columnPolicyMatcher) {
        assertColumnType(expectedTypeName);
        isExactlyInstanceOf(ObjectColumnType.class);
        ObjectColumnType<?> objectColumnType = (ObjectColumnType<?>) actual;

        assertThat(objectColumnType.parameters()).isEmpty();
        assertThat(objectColumnType.nestedColumns()).isEmpty();
        assertThat(objectColumnType.columnPolicy()).isNotEmpty()
            .get().as("columnPolicy").satisfies(columnPolicyMatcher);
        return this;
    }

    public NodeAssert isObjectColumnType(String expectedTypeName,
                                         Consumer<ColumnPolicy> columnPolicyMatcher,
                                         Consumer<Iterable<? extends Node>> nestedColumnsMatcher) {
        assertColumnType(expectedTypeName);
        isExactlyInstanceOf(ObjectColumnType.class);
        ObjectColumnType<?> objectColumnType = (ObjectColumnType<?>) actual;

        assertThat(objectColumnType.parameters()).isEmpty();
        assertThat(objectColumnType.nestedColumns()).satisfies(nestedColumnsMatcher);
        assertThat(objectColumnType.columnPolicy()).isNotEmpty()
            .get().as("columnPolicy").satisfies(columnPolicyMatcher);
        return this;
    }

    public NodeAssert isColumnDefinition(String expectedIdent, Consumer<Node> columnTypeMatcher) {
        isNotNull();
        isExactlyInstanceOf(ColumnDefinition.class);
        ColumnDefinition<?> columnDefinition = (ColumnDefinition<?>) actual;

        assertThat(columnDefinition.ident()).isEqualTo(expectedIdent);
        assertThat(columnDefinition.constraints()).isEmpty();
        assertThat(columnDefinition.defaultExpression()).isNull();
        assertThat(columnDefinition.generatedExpression()).isNull();
        assertThat(columnDefinition.type()).as("columnType").satisfies(columnTypeMatcher);
        return this;
    }

    private void assertColumnType(String expectedTypeName) {
        isNotNull();
        isInstanceOf(ColumnType.class);
        assertThat(((ColumnType<?>) actual).name()).as("typeName").isEqualTo(expectedTypeName);
    }
}
