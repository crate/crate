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

package io.crate.operation.aggregation.impl;

import io.crate.Streamer;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TDigestStateTest {

    @Test
    public void testStreaming() throws Exception {
        TDigestState digestState1 = new TDigestState(250, new double[]{0.5, 0.8});
        BytesStreamOutput out = new BytesStreamOutput();
        TDigestStateType digestStateType = TDigestStateType.INSTANCE;
        Streamer streamer = digestStateType.create().streamer();
        streamer.writeValueTo(out, digestState1);
        StreamInput in = out.bytes().streamInput();
        TDigestState digestState2 = (TDigestState) streamer.readValueFrom(in);

        assertEquals(digestState1.compression(), digestState2.compression(), 0.001d);
        assertEquals(digestState1.fractions()[0], digestState2.fractions()[0], 0.001d);
        assertEquals(digestState1.fractions()[1], digestState2.fractions()[1], 0.001d);
    }
}
