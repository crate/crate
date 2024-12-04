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

package io.crate.execution.dsl.projection;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.types.DataTypes;

public class UpdateProjectionTest {

    @Test
    public void testEquals() throws Exception {
        UpdateProjection u1 = new UpdateProjection(
            Literal.of(1), new String[]{"foo"}, new Symbol[]{Literal.of(1)}, new Symbol[]{new InputColumn(0, DataTypes.STRING)}, null, null);

        UpdateProjection u2 = new UpdateProjection(
            Literal.of(1), new String[]{"foo"}, new Symbol[]{Literal.of(1)},new Symbol[]{new InputColumn(0, DataTypes.STRING)}, null, null);

        assertThat(u2.equals(u1)).isTrue();
        assertThat(u1.equals(u2)).isTrue();
        assertThat(u1.hashCode()).isEqualTo(u2.hashCode());

    }

    @Test
    public void test_serialization() throws Exception {

        UpdateProjection expected = new UpdateProjection(
            Literal.of(1),
            new String[]{"foo"},
            new Symbol[]{Literal.of(1)},
            new Symbol[]{new InputColumn(0, DataTypes.STRING)},
            new Symbol[]{Literal.of(1)},
            null);

        BytesStreamOutput out = new BytesStreamOutput();
        expected.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        UpdateProjection result = new UpdateProjection(in);

        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void test_serialization_backward_compatibility() throws Exception {

        UpdateProjection expected = new UpdateProjection(
            Literal.of(1),
            new String[]{"foo"},
            new Symbol[]{Literal.of(1)},
            new Symbol[]{},
            new Symbol[]{Literal.of(1)},
            null);

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_4_0_0);
        expected.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_4_0_0);
        UpdateProjection result = new UpdateProjection(in);

        assertThat(result.uidSymbol).isEqualTo(expected.uidSymbol);
        assertThat(result.assignments()).isEqualTo(expected.assignments());
        assertThat(result.assignmentsColumns()).isEqualTo(expected.assignmentsColumns());
        assertThat(result.requiredVersion()).isEqualTo(expected.requiredVersion());

        //Pre 4.1 versions of UpdateProjection have default output fields set to a long representing
        //a count which need to set when reading from an pre 4.1 node.
        assertThat(result.outputs()).isEqualTo(List.of(new InputColumn(0, DataTypes.LONG)));
        assertThat(result.returnValues()).isEqualTo(null);
    }
}
