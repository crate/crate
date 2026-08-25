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

package io.crate.execution.engine.aggregation.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import io.crate.Streamer;

public class StringAggStateTest {

    @Test
    public void testStreaming() throws Exception {
        var state = new StringAgg.StringAggState();
        state.values().addAll(List.of("a", "b", "c", "d"));
        state.delimiters().addAll(Arrays.asList(new String[] {"|", ";", null, "~"}));

        BytesStreamOutput out = new BytesStreamOutput();
        Streamer<StringAgg.StringAggState> streamer = StringAgg.StringAggStateType.INSTANCE.streamer();
        streamer.writeValueTo(out, state);
        StreamInput in = out.bytes().streamInput();
        assertThat(streamer.readValueFrom(in)).isEqualTo(state);


        out = new BytesStreamOutput();
        out.setVersion(Version.V_6_4_4);
        streamer.writeValueTo(out, state);
        in = out.bytes().streamInput();
        in.setVersion(Version.V_6_4_4);
        assertThat(streamer.readValueFrom(in)).isEqualTo(state);
    }
}
