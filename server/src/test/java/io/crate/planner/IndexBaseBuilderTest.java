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

package io.crate.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.TreeMap;

import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.planner.fetch.IndexBaseBuilder;

public class IndexBaseBuilderTest {

    @Test
    public void testDoubleIndex() throws Exception {
        IndexBaseBuilder builder = new IndexBaseBuilder();

        builder.allocate("i1", IntArrayList.from(1, 4));
        builder.allocate("i2", IntArrayList.from(1, 2));
        builder.allocate("i1", IntArrayList.from(1, 5));
        builder.allocate("i1", IntArrayList.from(1, 3));
        builder.allocate("i3", IntArrayList.from(1, 3));

        TreeMap<String, Integer> bases = builder.build();
        assertThat(bases).hasSize(3);
        assertThat(bases.get("i1")).isEqualTo(0);
        assertThat(bases.get("i2")).isEqualTo(6);
        assertThat(bases.get("i3")).isEqualTo(9);
    }

    @Test
    public void testMaxShard() throws Exception {
        IndexBaseBuilder builder = new IndexBaseBuilder();

        builder.allocate("i1", IntArrayList.from(1, 4));
        builder.allocate("i2", IntArrayList.from(1, 5));
        TreeMap<String, Integer> bases = builder.build();
        assertThat(bases).hasSize(2);
        assertThat(bases.get("i1")).isEqualTo(0);
        assertThat(bases.get("i2")).isEqualTo(5);
    }
}
