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

package io.crate.execution.engine.aggregation.impl;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.data.Offset;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import io.crate.Streamer;

public class TDigestStateTest {

    @Test
    public void testStreaming() throws Exception {
        TDigestState digestState1 = new TDigestState(250, new double[]{0.5, 0.8});
        BytesStreamOutput out = new BytesStreamOutput();
        TDigestStateType digestStateType = TDigestStateType.INSTANCE;
        Streamer<TDigestState> streamer = digestStateType.streamer();
        streamer.writeValueTo(out, digestState1);
        StreamInput in = out.bytes().streamInput();
        TDigestState digestState2 = (TDigestState) streamer.readValueFrom(in);

        assertThat(digestState1.compression()).isEqualTo(digestState2.compression(), Offset.offset(0.001d));
        assertThat(digestState1.fractions()[0]).isEqualTo(digestState2.fractions()[0], Offset.offset(0.001d));
        assertThat(digestState1.fractions()[1]).isEqualTo(digestState2.fractions()[1], Offset.offset(0.001d));
    }
}
