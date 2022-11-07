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

package io.crate.execution.engine.collect.files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.crate.analyze.CopyFromParserProperties;
import io.crate.execution.dsl.phases.FileUriCollectPhase;

class LineParserTest {

    @Test
    void test_readFirstLine_can_skip_lines_without_header() throws IOException {
        var bufferedReader = skipAndReadFirstLine(
            new CopyFromParserProperties(false, false, ',', 1),
            FileUriCollectPhase.InputFormat.CSV,
            "1\n2\n3"
        );
        assertThat(bufferedReader.readLine()).isEqualTo("2"); // 1 is skipped
    }

    @Test
    void test_readFirstLine_can_skip_lines_with_header() throws IOException {
        var bufferedReader = skipAndReadFirstLine(
            new CopyFromParserProperties(false, true, ',', 1),
            FileUriCollectPhase.InputFormat.CSV,
            "line_to_skip\nheader\n1\n2\n3"
        );
        assertThat(bufferedReader.readLine()).isEqualTo("1"); // 'line_to_skip' is skipped, 'header' is parsed
    }

    @Test
    void test_readFirstLine_can_skip_all_lines_with_header() {
        assertThatThrownBy(
            () -> skipAndReadFirstLine(
                new CopyFromParserProperties(false, true, ',', 10),
                FileUriCollectPhase.InputFormat.CSV,
                "line_to_skip\nheader\n1\n2\n3"
            )).isInstanceOf(NullPointerException.class);
    }

    @Test
    void test_readFirstLine_can_skip_all_lines_without_header() throws IOException {
        var bufferedReader = skipAndReadFirstLine(
            new CopyFromParserProperties(false, false, ',', 10),
            FileUriCollectPhase.InputFormat.CSV,
            "1\n2\n3"
        );
        assertThat(bufferedReader.readLine()).isNull();
    }

    @Test
    void test_readFirstLine_can_skip_no_lines_with_header() throws IOException {
        var bufferedReader = skipAndReadFirstLine(
            new CopyFromParserProperties(false, true, ',', -1), // skipNumLines <= 0 should have the same effect.
            FileUriCollectPhase.InputFormat.CSV,
            "header\n1\n2\n3"
        );
        assertThat(bufferedReader.readLine()).isEqualTo("1");
    }

    @Test
    void test_readFirstLine_can_skip_lines_from_JSON_inputs() throws IOException {
        var bufferedReader = skipAndReadFirstLine(
            new CopyFromParserProperties(false, true, ',', 1),
            FileUriCollectPhase.InputFormat.JSON,
            "{\"x\":1}\n{\"x\":2}\n"
        );
        assertThat(bufferedReader.readLine()).isEqualTo("{\"x\":2}");
    }

    private BufferedReader skipAndReadFirstLine(CopyFromParserProperties properties, FileUriCollectPhase.InputFormat inputFormat, String lines) throws IOException {
        LineParser lineParser = new LineParser(properties, List.of());
        StringReader stringReader = new StringReader(lines);
        BufferedReader bufferedReader = new BufferedReader(stringReader);
        lineParser.readFirstLine(URI.create("dummy"), inputFormat, bufferedReader);
        return bufferedReader;
    }
}
