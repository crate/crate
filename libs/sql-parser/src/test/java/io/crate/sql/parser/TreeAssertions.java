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

package io.crate.sql.parser;

import static io.crate.sql.SqlFormatter.formatSql;
import static io.crate.sql.parser.SqlParser.createStatement;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import io.crate.common.collections.Lists;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.Statement;

final class TreeAssertions {
    private TreeAssertions() {}

    static void assertFormattedSql(Node expected) {
        String formatted = formatSql(expected);

        // verify round-trip of formatting already-formatted SQL
        Statement actual = parseFormatted(formatted, expected);
        assertThat(formatSql(actual)).isEqualTo(formatted);

        // compare parsed tree with parsed tree of formatted SQL
        if (!actual.equals(expected)) {
            // simplify finding the non-equal part of the tree
            assertListEquals(linearizeTree(actual), linearizeTree(expected));
        }
        assertThat(actual).isEqualTo(expected);
    }

    private static Statement parseFormatted(String sql, Node tree) {
        try {
            return createStatement(sql);
        } catch (ParsingException e) {
            throw new AssertionError(format(
                "failed to parse formatted SQL: %s\nerror: %s\ntree: %s", sql, e.getMessage(), tree));
        }
    }

    private static List<Node> linearizeTree(Node tree) {
        ArrayList<Node> nodes = new ArrayList<>();
        new DefaultTraversalVisitor<Node, Void>() {
            void process(Node node) {
                node.accept(this, null);
                nodes.add(node);
            }
        }.process(tree);
        return nodes;
    }

    private static <T> void assertListEquals(List<T> actual, List<T> expected) {
        assertThat(actual)
            .as(
                format(
                    "Lists not equal%nActual [%s]:%n    %s%nExpected [%s]:%n    %s",
                    actual.size(), Lists.joinOn("\n    ", actual, Object::toString),
                    expected.size(), Lists.joinOn("\n    ", expected, Object::toString)))
            .hasSize(expected.size());

        assertThat(actual).isEqualTo(expected);
    }
}
