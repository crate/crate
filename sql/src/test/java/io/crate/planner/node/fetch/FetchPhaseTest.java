/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.node.fetch;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.crate.analyze.symbol.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.planner.node.ExecutionPhases;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.util.TreeMap;
import java.util.UUID;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class FetchPhaseTest {

    @Test
    public void testStreaming() throws Exception {

        TableIdent t1 = new TableIdent(null, "t1");

        TreeMap<String, Integer> bases = new TreeMap<String, Integer>();
        bases.put(t1.name(), 0);
        bases.put("i2", 1);

        Multimap<TableIdent, String> tableIndices = HashMultimap.create();
        tableIndices.put(t1, t1.name());
        tableIndices.put(new TableIdent(null, "i2"), "i2_s1");
        tableIndices.put(new TableIdent(null, "i2"), "i2_s2");

        ReferenceIdent nameIdent = new ReferenceIdent(t1, "name");
        Reference name = new Reference(new ReferenceInfo(nameIdent, RowGranularity.DOC, DataTypes.STRING));

        FetchPhase orig = new FetchPhase(
                UUID.randomUUID(),
                1,
                ImmutableSet.<String>of("node1", "node2"),
                bases,
                tableIndices,
                ImmutableList.of(name)
        );

        BytesStreamOutput out = new BytesStreamOutput();
        ExecutionPhases.toStream(out, orig);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        FetchPhase streamed = (FetchPhase) ExecutionPhases.fromStream(in);

        assertThat(orig.jobId(), is(streamed.jobId()));
        assertThat(orig.executionPhaseId(), is(streamed.executionPhaseId()));
        assertThat(orig.executionNodes(), is(streamed.executionNodes()));
        assertThat(orig.fetchRefs(), is(streamed.fetchRefs()));
        assertThat(orig.bases(), is(streamed.bases()));
        assertThat(orig.tableIndices(), is(streamed.tableIndices()));

    }
}