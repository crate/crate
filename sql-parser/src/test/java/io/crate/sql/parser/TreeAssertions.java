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

package io.crate.sql.parser;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.crate.sql.tree.*;

import javax.annotation.Nullable;
import java.util.List;

import static io.crate.sql.SqlFormatter.formatSql;
import static io.crate.sql.parser.SqlParser.createStatement;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public final class TreeAssertions
{
    private TreeAssertions() {}

    public static void assertFormattedSql(Node expected)
    {
        String formatted = formatSql(expected);

        // verify round-trip of formatting already-formatted SQL
        assertEquals(formatSql(createStatement(formatted)), formatted);

        // compare parsed tree with parsed tree of formatted SQL
        Statement actual = createStatement(formatted);
        if (!actual.equals(expected)) {
            // simplify finding the non-equal part of the tree
            assertListEquals(linearizeTree(actual), linearizeTree(expected));
        }
        assertEquals(actual, expected);
    }

    private static List<Node> linearizeTree(Node tree)
    {
        final ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        new DefaultTraversalVisitor<Node, Void>()
        {
            @Override
            public Node process(Node node, @Nullable Void context)
            {
                Node result = super.process(node, context);
                nodes.add(node);
                return result;
            }
        }.process(tree, null);
        return nodes.build();
    }

    private static <T> void assertListEquals(List<T> actual, List<T> expected)
    {
        if (actual.size() != expected.size()) {
            Joiner joiner = Joiner.on("\n    ");
            fail(format("Lists not equal%nActual [%s]:%n    %s%nExpected [%s]:%n    %s",
                    actual.size(), joiner.join(actual),
                    expected.size(), joiner.join(expected)));
        }
        assertEquals(actual, expected);
    }
}
