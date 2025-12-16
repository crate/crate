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

package io.crate.execution.dsl.phases;

import static io.crate.testing.TestingHelpers.createReference;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.analyze.CopyFromParserProperties;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;

public class FileUriCollectPhaseTest {

    @Test
    public void test_streaming_of_file_uri_collect_phase() throws IOException {
        var expected = new FileUriCollectPhase(
            UUID.randomUUID(),
            0,
            "test",
            Collections.singletonList("noop_id"),
            Literal.of("uri"),
            List.of(),
            List.of(createReference("name", DataTypes.STRING)),
            Collections.emptyList(),
            null,
            false,
            new CopyFromParserProperties(true, true, '|', 0),
            FileUriCollectPhase.InputFormat.CSV,
            Settings.EMPTY
        );

        BytesStreamOutput output = new BytesStreamOutput();
        expected.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        var actual = new FileUriCollectPhase(input);

        assertThat(expected.parserProperties().emptyStringAsNull()).isTrue();
        assertThat(actual.parserProperties().columnSeparator()).isEqualTo('|');
        assertThat(expected).isEqualTo(actual);
    }
}
