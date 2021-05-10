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

package io.crate.execution.engine.collect.files;

import io.crate.analyze.CopyFromParserProperties;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.operation.collect.files.CSVLineParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class LineParser {

    private final CopyFromParserProperties parserProperties;
    private CSVLineParser csvLineParser;

    private InputType inputType;

    public LineParser(CopyFromParserProperties parserProperties) {
        this.parserProperties = parserProperties;
    }

    private enum InputType {
        CSV,
        JSON
    }

    public void readFirstLine(URI currentUri,
                              FileUriCollectPhase.InputFormat inputFormat,
                              BufferedReader currentReader) throws IOException {
        if (isInputCsv(inputFormat, currentUri)) {
            csvLineParser = new CSVLineParser(parserProperties);
            csvLineParser.parseHeader(currentReader.readLine());
            inputType = InputType.CSV;
        } else {
            inputType = InputType.JSON;
        }
    }

    public byte[] getByteArray(String line) throws IOException {
        if (inputType == InputType.CSV) {
            return csvLineParser.parse(line);
        } else {
            return line.getBytes(StandardCharsets.UTF_8);
        }
    }

    private boolean isInputCsv(FileUriCollectPhase.InputFormat inputFormat, URI currentUri) {
        return (inputFormat == FileUriCollectPhase.InputFormat.CSV) || currentUri.toString().endsWith(".csv");
    }
}
