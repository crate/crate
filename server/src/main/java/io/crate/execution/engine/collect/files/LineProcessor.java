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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

import io.crate.analyze.CopyFromParserProperties;
import io.crate.data.BatchIterator;
import io.crate.data.Input;
import io.crate.data.MappedForwardingBatchIterator;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.execution.dsl.phases.FileUriCollectPhase.InputFormat;
import io.crate.execution.engine.collect.files.FileReadingIterator.LineCursor;
import io.crate.expression.InputRow;
import io.crate.expression.reference.file.LineContext;
import io.crate.operation.collect.files.CSVLineParser;

public final class LineProcessor extends MappedForwardingBatchIterator<LineCursor, Row> {

    private final BatchIterator<LineCursor> source;
    private final LineContext lineContext;
    private final CopyFromParserProperties parserProperties;
    private final List<String> targetColumns;
    private final InputRow row;

    private InputFormat inputFormat;
    private CSVLineParser csvLineParser;
    private boolean firstLine = true;

    public LineProcessor(BatchIterator<LineCursor> source,
                         List<Input<?>> inputs,
                         List<LineCollectorExpression<?>> expressions,
                         FileUriCollectPhase.InputFormat inputFormat,
                         CopyFromParserProperties parserProperties,
                         List<String> targetColumns) {
        this.source = source;
        this.inputFormat = inputFormat;
        this.row = new InputRow(inputs);
        this.parserProperties = parserProperties;
        this.targetColumns = targetColumns;
        this.lineContext = new LineContext(source.currentElement());
        for (LineCollectorExpression<?> collectorExpression : expressions) {
            collectorExpression.startCollect(lineContext);
        }
    }

    @Override
    public void moveToStart() {
        source.moveToStart();
        firstLine = true;
    }

    private boolean readFirstLine(URI currentUri, String line) throws IOException {
        if (isCSV(inputFormat, currentUri)) {
            csvLineParser = new CSVLineParser(parserProperties, targetColumns);
            inputFormat = InputFormat.CSV;
            if (parserProperties.fileHeader()) {
                csvLineParser.parseHeader(line);
                return true;
            }
        } else {
            inputFormat = InputFormat.JSON;
        }
        return false;
    }

    private byte[] getByteArray(String line, long rowNumber) throws IOException {
        if (inputFormat == InputFormat.CSV) {
            return parserProperties.fileHeader() ?
                csvLineParser.parse(line, rowNumber) : csvLineParser.parseWithoutHeader(line, rowNumber);
        } else {
            return line.getBytes(StandardCharsets.UTF_8);
        }
    }

    private static boolean isCSV(FileUriCollectPhase.InputFormat inputFormat, URI currentUri) {
        return (inputFormat == FileUriCollectPhase.InputFormat.CSV) || currentUri.toString().endsWith(".csv");
    }

    @Override
    public boolean moveNext() {
        try {
            while (source.moveNext()) {
                LineCursor cursor = source.currentElement();
                String line = cursor.line();
                if (line == null) {
                    assert cursor.failure() != null : "If the line is null, there must be a failure";
                    return true;
                }
                if (firstLine) {
                    firstLine = false;
                    if (readFirstLine(cursor.uri(), line)) {
                        continue;
                    }
                }
                try {
                    byte[] json = getByteArray(line, cursor.lineNumber());
                    lineContext.resetCurrentParsingFailure();
                    lineContext.rawSource(json);
                } catch (Throwable parseError) {
                    lineContext.setCurrentParsingFailure(parseError.getMessage());
                }
                return true;
            }
            return false;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Row currentElement() {
        return row;
    }

    @Override
    protected BatchIterator<LineCursor> delegate() {
        return source;
    }
}
