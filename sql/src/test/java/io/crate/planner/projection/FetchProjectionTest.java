/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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

package io.crate.planner.projection;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.util.TreeMap;

import static io.crate.testing.TestingHelpers.createReference;

public class FetchProjectionTest extends CrateUnitTest {

    @Test
    public void testStreaming() throws Exception {
        IntObjectOpenHashMap<String> readerNodes = new IntObjectOpenHashMap(1);
        readerNodes.put(0, "node1");
        TreeMap<Integer, String> readerIndices = (TreeMap)TreeMapBuilder.<Integer, String>newMapBuilder().put(0, "index1").map();

        FetchProjection p = new FetchProjection(
                1,
                Literal.newLiteral(1L),
                ImmutableList.<Symbol>of(new InputColumn(1)),
                ImmutableList.of(createReference("bar", DataTypes.STRING).info()),
                Sets.newHashSet("node1"),
                readerNodes,
                readerIndices
        );

        BytesStreamOutput out = new BytesStreamOutput();
        Projection.toStream(p, out);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        FetchProjection p2 = (FetchProjection) Projection.fromStream(in);

        assertEquals(p, p2);
    }
}
