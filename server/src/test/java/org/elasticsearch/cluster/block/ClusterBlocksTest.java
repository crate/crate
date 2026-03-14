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

package org.elasticsearch.cluster.block;

import static io.crate.testing.Asserts.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class ClusterBlocksTest extends ESTestCase {

    @Test
    public void test_serialization() throws IOException {
        Set<ClusterBlock> global = Set.of(ClusterBlockTests.randomClusterBlock());
        Map<Integer, Set<ClusterBlock>> tablesBlocks = new HashMap<>();
        Map<String, Set<ClusterBlock>> indicesBlocks = new HashMap<>();
        tablesBlocks.put(1, Set.of(ClusterBlockTests.randomClusterBlock()));
        indicesBlocks.put("dummy", Set.of(ClusterBlockTests.randomClusterBlock()));

        ClusterBlocks clusterBlocks = new ClusterBlocks(global, tablesBlocks, indicesBlocks);

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_6_3_0);
        clusterBlocks.writeTo(out);
        var in = out.bytes().streamInput();
        in.setVersion(Version.V_6_3_0);

        ClusterBlocks actual = ClusterBlocks.readFrom(in);

        assertThat(actual.global()).isEqualTo(global);
        assertThat(actual.tables()).isEqualTo(tablesBlocks);
        assertThat(actual.indices()).isEqualTo(indicesBlocks);

        out = new BytesStreamOutput();
        out.setVersion(Version.V_6_2_0);
        clusterBlocks.writeTo(out);
        in = out.bytes().streamInput();
        in.setVersion(Version.V_6_2_0);

        actual = ClusterBlocks.readFrom(in);

        assertThat(actual.global()).isEqualTo(global);
        assertThat(actual.tables()).isEmpty();
        assertThat(actual.indices()).isEqualTo(indicesBlocks);
    }
}
