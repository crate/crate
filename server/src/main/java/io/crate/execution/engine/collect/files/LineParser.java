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
import org.jetbrains.annotations.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

public class LineParser {

    private final CopyFromParserProperties parserProperties;
    private final List<String> targetColumns;
    private CSVLineParser csvLineParser;

    private InputType inputType;

    public LineParser(CopyFromParserProperties parserProperties, List<String> targetColumns) {
        this.parserProperties = parserProperties;
        this.targetColumns = targetColumns;
    }

    public enum InputType {
        CSV,
        JSON
    }

    public InputType inputType() {
        return inputType;
    }

    /**
     * @return all actual columns (including non-existing in the target table) so that new columns can be added to the target columns
     * or NULL if no need to update target columns.
     *
     * Also used to detect which columns doesn't have
     */
    @Nullable
    public Set<String> readFirstLine(URI currentUri,
                                     FileUriCollectPhase.InputFormat inputFormat,
                                     BufferedReader currentReader) throws IOException {
        for (long i = 0; i < parserProperties.skipNumLines(); i++) {
            currentReader.readLine();
        }
        if (isInputCsv(inputFormat, currentUri)) {
            csvLineParser = new CSVLineParser(parserProperties, targetColumns);
            inputType = InputType.CSV;
            if (parserProperties.fileHeader()) {
                return Set.of(csvLineParser.parseHeader(currentReader.readLine()));
            } else {
                // if CSV doesn't have header, we explicitly set target columns to table columns on planning stage,
                // no need to update context and adjust plan.
                return null;
            }
        } else {
            inputType = InputType.JSON;
            return null; // JSON has no header, so we adjust target columns later during regular processing.
        }
    }

    public byte[] getByteArray(String line, long rowNumber) throws IOException {
        if (inputType == InputType.CSV) {
            return parserProperties.fileHeader() ?
                csvLineParser.parse(line, rowNumber) : csvLineParser.parseWithoutHeader(line, rowNumber);
        } else {
            return line.getBytes(StandardCharsets.UTF_8);
        }
    }

    private boolean isInputCsv(FileUriCollectPhase.InputFormat inputFormat, URI currentUri) {
        return (inputFormat == FileUriCollectPhase.InputFormat.CSV) || currentUri.toString().endsWith(".csv");
    }
}
