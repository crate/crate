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

package io.crate.core.collections.nested;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.elasticsearch.common.Nullable;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class NestedTraverserTest {

    static class TestNested implements Nested<TestNested> {

        private final List<TestNested> children;
        private final int value;

        public TestNested(int value, @Nullable List<TestNested> children) {
            this.value = value;
            this.children = Objects.firstNonNull(children, ImmutableList.<TestNested>of());
        }

        @Override
        public Collection<TestNested> children() {
            return this.children;
        }

        public int value() {
            return value;
        }
    }


    @Test
    public void testNestedTraverser() throws Exception {
        TestNested nested = new TestNested(0, Lists.newArrayList(
                new TestNested(1, null),
                new TestNested(2,
                        Lists.newArrayList(
                                new TestNested(3, null),
                                new TestNested(4, null),
                                new TestNested(5, null)
                        )
                )
        ));
        NestedTraverser<TestNested> traverser = new NestedTraverser<>();
        StringBuilder builder = new StringBuilder();
        for (TestNested node : traverser.preOrderTraversal(nested)) {
            builder.append(node.value());
        }
        assertThat(builder.toString(), is("012345"));

        builder = new StringBuilder();
        for (TestNested node : traverser.postOrderTraversal(nested)) {
            builder.append(node.value());
        }
        assertThat(builder.toString(), is("134520"));
    }
}
