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
package io.crate.planner.node;

import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.where.DocKeys;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.PrimaryKeyLookupPhase;
import io.crate.planner.projection.EvalProjection;
import io.crate.planner.projection.Projection;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class PrimaryKeyLookupPhaseTest extends CrateUnitTest {

    @Test
    public void testStreaming() throws IOException {

        Random random = new Random();

        UUID uuid = UUID.randomUUID();
        int executionNodeId = random.nextInt();
        String name = "name";
        RowGranularity rowGranularity = RowGranularity.CLUSTER;
        Map<ColumnIdent, Integer> pkMapping = new HashMap<>();
        Map<String, Map<ShardId, List<DocKeys.DocKey>>> docKeyShardMap = new HashMap<>();
        TableIdent tableIdent = new TableIdent("schema", "name");
        List<Symbol> toCollect = new ArrayList<>();

        List<Projection> projections = new ArrayList<>();
        List<List<Symbol>> symbolLists = new ArrayList<>();
        symbolLists.add(Arrays.asList(Literal.of(1), Literal.of("test")));
        int clusteredByIdx = 0;
        List<Integer> partitionedByIdx = new ArrayList<>();
        DocKeys docKeys =
            new DocKeys(
                symbolLists,
                false,
                clusteredByIdx,
                partitionedByIdx);

        DistributionInfo distributionInfo = DistributionInfo.DEFAULT_BROADCAST;

        checkByteEquality(
            new PrimaryKeyLookupPhase(
                uuid,
                executionNodeId,
                name,
                rowGranularity,
                pkMapping,
                docKeyShardMap,
                tableIdent,
                false,
                toCollect,
                projections,
                docKeys,
                distributionInfo));

        // add more data to test for more serialization errors

        pkMapping.put(new ColumnIdent("testcol"), 1);
        docKeyShardMap = new HashMap<>();
        Map<ShardId, List<DocKeys.DocKey>> shardMap = new HashMap<>();
        shardMap.put(
            new ShardId("index", "uuid", 1),
            Collections.singletonList(
                docKeys.getOnlyKey()));
        docKeyShardMap.put("node", shardMap);
        toCollect = new ArrayList<>();
        toCollect.add(Literal.of("testsymbol"));

        projections.add(new EvalProjection(Collections.emptyList()));
        symbolLists.clear();
        symbolLists.add(Arrays.asList(Literal.of(1), Literal.of("test"), Literal.of(3)));
        symbolLists.add(Arrays.asList(Literal.of(4), Literal.of("test2"), Literal.of(8)));
        clusteredByIdx = 1;
        partitionedByIdx.add(1);
        docKeys =
            new DocKeys(
                symbolLists,
                true,
                clusteredByIdx,
                partitionedByIdx);

        checkByteEquality(
            new PrimaryKeyLookupPhase(
                uuid,
                executionNodeId,
                name,
                rowGranularity,
                pkMapping,
                docKeyShardMap,
                tableIdent,
                true,
                toCollect,
                projections,
                docKeys,
                distributionInfo));
    }

    private static void checkByteEquality(PrimaryKeyLookupPhase pkLookupPhase) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        pkLookupPhase.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        PrimaryKeyLookupPhase pkLookupPhase2 = new PrimaryKeyLookupPhase(in);

        BytesStreamOutput copy = new BytesStreamOutput();
        pkLookupPhase2.writeTo(copy);

        assertTrue(copy.bytes().equals(out.bytes()));
    }
}
