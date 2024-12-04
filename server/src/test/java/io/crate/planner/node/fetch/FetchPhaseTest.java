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

package io.crate.planner.node.fetch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import io.crate.execution.dsl.phases.ExecutionPhases;
import io.crate.execution.dsl.phases.FetchPhase;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SimpleReference;
import io.crate.types.DataTypes;

public class FetchPhaseTest {

    @Test
    public void testStreaming() throws Exception {

        RelationName t1 = new RelationName(Schemas.DOC_SCHEMA_NAME, "t1");

        TreeMap<String, Integer> bases = new TreeMap<>();
        bases.put(t1.name(), 0);
        bases.put("i2", 1);

        HashMap<RelationName, Collection<String>> tableIndices = new HashMap<>();
        tableIndices.put(t1, List.of(t1.name()));
        tableIndices.put(new RelationName(Schemas.DOC_SCHEMA_NAME, "i2"), List.of("i2_s1", "i2_s2"));

        ReferenceIdent nameIdent = new ReferenceIdent(t1, "name");
        SimpleReference name = new SimpleReference(nameIdent, RowGranularity.DOC, DataTypes.STRING, 0, null);

        FetchPhase orig = new FetchPhase(
            1,
            Set.of("node1", "node2"),
            bases,
            tableIndices,
            List.of(name)
        );

        BytesStreamOutput out = new BytesStreamOutput();
        ExecutionPhases.toStream(out, orig);

        StreamInput in = out.bytes().streamInput();
        FetchPhase streamed = (FetchPhase) ExecutionPhases.fromStream(in);

        assertThat(orig.phaseId()).isEqualTo(streamed.phaseId());
        assertThat(orig.nodeIds()).isEqualTo(streamed.nodeIds());
        assertThat(orig.fetchRefs()).isEqualTo(streamed.fetchRefs());
        assertThat(orig.bases()).isEqualTo(streamed.bases());
        assertThat(orig.tableIndices()).isEqualTo(streamed.tableIndices());

    }
}
