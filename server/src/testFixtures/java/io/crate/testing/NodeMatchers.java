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

package io.crate.testing;

import io.crate.sql.tree.CollectionColumnType;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.ObjectColumnType;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Optional;

import static io.crate.testing.MoreMatchers.withFeature;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class NodeMatchers {

    public static Matcher<Node> isCollectionColumnType(String typeName, Matcher<Node> innerTypeMatcher) {
        return allOf(
            isColumnType(typeName),
            instanceOf(CollectionColumnType.class),
            withFeature(x -> ((CollectionColumnType) x).innerType(), "innerType", innerTypeMatcher)
        );
    }

    public static Matcher<ColumnPolicy> isColumnPolicy(ColumnPolicy columnPolicy) {
        return allOf(
            instanceOf(ColumnPolicy.class),
            withFeature(x -> ((ColumnPolicy) x).lowerCaseName(), "lowerCaseName", is(columnPolicy.lowerCaseName()))
        );
    }

    public static Matcher<Node> isObjectColumnType(String typeName, Matcher<ColumnPolicy> columnPolicyMatcher) {
        return allOf(
            isColumnType(typeName),
            instanceOf(ObjectColumnType.class),
            withFeature(x -> ((ObjectColumnType) x).nestedColumns(), "nestedColumns", is(List.of())),
            withFeature(x -> ((Optional<ColumnPolicy>) ((ObjectColumnType) x).objectType()).get(),
                        "columnPolicy", columnPolicyMatcher)
        );
    }

    public static Matcher<Node> isObjectColumnType(String typeName,
                                                   Matcher<ColumnPolicy> columnPolicyMatcher,
                                                   Matcher<Iterable<? extends Node>> nestedColumnsMatcher) {
        return allOf(
            isColumnType(typeName),
            instanceOf(ObjectColumnType.class),
            withFeature(x -> ((ObjectColumnType) x).nestedColumns(), "nestedColumns", nestedColumnsMatcher),
            withFeature(x -> ((Optional<ColumnPolicy>) ((ObjectColumnType) x).objectType()).get(),
                        "columnPolicy", columnPolicyMatcher)
        );
    }

    public static Matcher<Node> isColumnType(String typeName) {
        return allOf(
            instanceOf(ColumnType.class),
            withFeature(x -> ((ColumnType) x).name(), "typeName", is(typeName)),
            withFeature(x -> ((ColumnType) x).parameters(), "parameters", is(List.of()))
        );
    }

    public static Matcher<Node> isColumnType(String typeName, Matcher<Iterable<? extends Integer>> paramsMatcher) {
        return allOf(
            instanceOf(ColumnType.class),
            withFeature(x -> ((ColumnType) x).name(), "typeName", is(typeName)),
            withFeature(x -> ((ColumnType) x).parameters(), "parameters", paramsMatcher)
        );
    }

    public static Matcher<Node> isColumnDefinition(String ident, Matcher<Node> columnTypeMatcher) {
        return allOf(
            instanceOf(ColumnDefinition.class),
            withFeature(x -> ((ColumnDefinition) x).ident(), "ident", is(ident)),
            withFeature(x -> ((ColumnDefinition) x).constraints(), "constraints", is(List.of())),
            withFeature(x -> ((ColumnDefinition) x).defaultExpression(), "defaultExpression", is(nullValue())),
            withFeature(x -> ((ColumnDefinition) x).generatedExpression(), "generatedExpression", is(nullValue())),
            withFeature(x -> ((ColumnDefinition) x).type(), "columnType", columnTypeMatcher)
        );
    }
}
