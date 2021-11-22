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


import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.crate.analyze.CopyFromParserProperties;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class FileUriCollectPhaseTest {

    @Test
    public void test_streaming_of_file_uri_collect_phase_before_4_4_0() throws IOException {
        var expected = new FileUriCollectPhase(
            UUID.randomUUID(),
            0,
            "test",
            Collections.singletonList("noop_id"),
            Literal.of("uri"),
            List.of(createReference("name", DataTypes.STRING)),
            Collections.emptyList(),
            null,
            false,
            new CopyFromParserProperties(true, '|'),
            FileUriCollectPhase.InputFormat.CSV,
            null
        );

        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(Version.V_4_3_0);
        expected.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        input.setVersion(Version.V_4_3_0);
        var actual = new FileUriCollectPhase(input);

        assertThat(expected.nodeIds(), is(actual.nodeIds()));
        assertThat(expected.distributionInfo(), is(actual.distributionInfo()));
        assertThat(expected.targetUri(), is(actual.targetUri()));
        assertThat(expected.toCollect(), is(actual.toCollect()));

        // parser properties option serialization implemented in crate >= 4.4.0
        assertThat(expected.parserProperties().emptyStringAsNull(), is(true));
        assertThat(actual.parserProperties().emptyStringAsNull(), is(false));

        assertThat(actual.parserProperties().columnSeparator(), is(CsvSchema.DEFAULT_COLUMN_SEPARATOR));

        assertThat(expected.inputFormat(), is(actual.inputFormat()));
        assertThat(expected.compression(), is(actual.compression()));
        assertThat(expected.sharedStorage(), is(actual.sharedStorage()));
    }

    @Test
    public void test_streaming_of_file_uri_collect_phase_after_or_on_4_4_0() throws IOException {
        var expected = new FileUriCollectPhase(
            UUID.randomUUID(),
            0,
            "test",
            Collections.singletonList("noop_id"),
            Literal.of("uri"),
            List.of(createReference("name", DataTypes.STRING)),
            Collections.emptyList(),
            null,
            false,
            new CopyFromParserProperties(true, '|'),
            FileUriCollectPhase.InputFormat.CSV,
            null
        );

        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(Version.V_4_4_0);
        expected.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        input.setVersion(Version.V_4_4_0);
        var actual = new FileUriCollectPhase(input);

        assertThat(expected.parserProperties().emptyStringAsNull(), is(true));
        assertThat(actual.parserProperties().columnSeparator(), is('|'));
        assertThat(expected, is(actual));
    }

    @Test
    public void test_streaming_of_file_uri_collect_phase_before_4_7_0() throws IOException {
        var expected = new FileUriCollectPhase(
            UUID.randomUUID(),
            0,
            "test",
            Collections.singletonList("noop_id"),
            Literal.of("uri"),
            List.of(createReference("name", DataTypes.STRING)),
            Collections.emptyList(),
            null,
            false,
            new CopyFromParserProperties(true, '|'),
            FileUriCollectPhase.InputFormat.CSV,
            null
        );

        var actualInput = new FileUriCollectPhase(
            UUID.randomUUID(),
            0,
            "test",
            Collections.singletonList("noop_id"),
            Literal.of("uri"),
            List.of(createReference("name", DataTypes.STRING)),
            Collections.emptyList(),
            null,
            false,
            new CopyFromParserProperties(true, '|'),
            FileUriCollectPhase.InputFormat.CSV,
            "protocol"
        );

        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(Version.V_4_4_0);
        actualInput.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        input.setVersion(Version.V_4_4_0);
        var actual = new FileUriCollectPhase(input);

        assertThat(actual, is(expected));
    }

    @Test
    public void test_streaming_of_file_uri_collect_phase_after_4_7_0() throws IOException {
        var expected = new FileUriCollectPhase(
            UUID.randomUUID(),
            0,
            "test",
            Collections.singletonList("noop_id"),
            Literal.of("uri"),
            List.of(createReference("name", DataTypes.STRING)),
            Collections.emptyList(),
            null,
            false,
            new CopyFromParserProperties(true, '|'),
            FileUriCollectPhase.InputFormat.CSV,
            null
        );

        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(Version.V_4_7_0);
        expected.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        input.setVersion(Version.V_4_7_0);
        var actual = new FileUriCollectPhase(input);

        assertThat(actual, is(expected));
    }
}
