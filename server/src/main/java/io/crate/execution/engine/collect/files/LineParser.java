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
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.List;

public class LineParser {

    private final CopyFromParserProperties parserProperties;
    private final List<String> targetColumns;
    private CSVLineParser csvLineParser;

    private InputType inputType;

    public LineParser(CopyFromParserProperties parserProperties, @Nonnull List<String> targetColumns) {
        this.parserProperties = parserProperties;
        this.targetColumns = targetColumns;
    }

    private enum InputType {
        CSV,
        JSON
    }

    public List<String> targetColumns() {
        return targetColumns;
    }

    public void readFirstLine(URI currentUri,
                              FileUriCollectPhase.InputFormat inputFormat,
                              BufferedReader currentReader) throws IOException {
        for (long i = 0; i < parserProperties.skipNumLines(); i++) {
            currentReader.readLine();
        }
        if (isInputCsv(inputFormat, currentUri)) {
            csvLineParser = new CSVLineParser(parserProperties, targetColumns);
            if (parserProperties.fileHeader()) {
                csvLineParser.parseHeader(currentReader.readLine());
            }
            inputType = InputType.CSV;
        } else {
            inputType = InputType.JSON;
        }
    }

    /**
     * @return values of the targetColumns, k-th element of the result contains value of the k-th target column.
     */
    public Object[] getValues(String line, long rowNumber) throws IOException {
        if (inputType == InputType.CSV) {
            return new Object[]{};
            // TODO: line.split(delimiter) but there are some options, maybe let csvLineParser return object[] so that it still handles header and options.
//            return parserProperties.fileHeader() ?
//                csvLineParser.parse(line, rowNumber) : csvLineParser.parseWithoutHeader(line, rowNumber);
        } else {
            var jsonMap =  JsonXContent.JSON_XCONTENT
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, line)
                .map();
            Object[] values = new Object[targetColumns.size()];
            for (int i = 0; i < targetColumns.size(); i++) {
                values[i] = jsonMap.get(targetColumns.get(i));
            }
            return values;
        }
    }

    private boolean isInputCsv(FileUriCollectPhase.InputFormat inputFormat, URI currentUri) {
        return (inputFormat == FileUriCollectPhase.InputFormat.CSV) || currentUri.toString().endsWith(".csv");
    }
}
