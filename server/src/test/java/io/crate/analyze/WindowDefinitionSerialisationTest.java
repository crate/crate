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

package io.crate.analyze;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.sql.tree.FrameBound;
import io.crate.sql.tree.WindowFrame;

public class WindowDefinitionSerialisationTest {

    @Test
    public void testSerialisationWithMissingFields() throws IOException {
        FrameBoundDefinition start = new FrameBoundDefinition(FrameBound.Type.UNBOUNDED_PRECEDING, Literal.NULL);
        WindowFrameDefinition frameDefinition = new WindowFrameDefinition(WindowFrame.Mode.RANGE, start, null);
        WindowDefinition windowDefinition = new WindowDefinition(singletonList(Literal.of(2L)), null, frameDefinition);

        BytesStreamOutput output = new BytesStreamOutput();
        windowDefinition.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        WindowDefinition fromInputWindowDefinition = new WindowDefinition(input);

        assertThat(fromInputWindowDefinition).isEqualTo(windowDefinition);
        assertThat(fromInputWindowDefinition.hashCode()).isEqualTo(windowDefinition.hashCode());
    }

    @Test
    public void testFullyConstructedWindowDefinitionSerialisation() throws IOException {
        FrameBoundDefinition start = new FrameBoundDefinition(FrameBound.Type.UNBOUNDED_PRECEDING, Literal.of(5L));
        FrameBoundDefinition end = new FrameBoundDefinition(FrameBound.Type.FOLLOWING, Literal.of(3L));
        WindowFrameDefinition frameDefinition = new WindowFrameDefinition(WindowFrame.Mode.RANGE, start, end);
        OrderBy orderBy = new OrderBy(singletonList(Literal.of(1L)), new boolean[]{true}, new boolean[]{true});
        WindowDefinition windowDefinition = new WindowDefinition(singletonList(Literal.of(2L)), orderBy, frameDefinition);

        BytesStreamOutput output = new BytesStreamOutput();
        windowDefinition.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        WindowDefinition fromInputWindowDefinition = new WindowDefinition(input);

        assertThat(fromInputWindowDefinition).isEqualTo(windowDefinition);
        assertThat(fromInputWindowDefinition.hashCode()).isEqualTo(windowDefinition.hashCode());
    }
}
