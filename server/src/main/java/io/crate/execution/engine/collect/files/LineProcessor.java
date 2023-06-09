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
import io.crate.execution.dsl.phases.FileUriCollectPhase.InputFormat;
import io.crate.expression.reference.file.LineContext;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;

public final class LineProcessor {

    private final LineContext lineContext = new LineContext();
    private final LineParser lineParser;

    public LineProcessor(CopyFromParserProperties parserProperties, List<String> targetColumns) {
        lineParser = new LineParser(parserProperties, targetColumns);
    }

    public void startCollect(Iterable<LineCollectorExpression<?>> collectorExpressions) {
        for (LineCollectorExpression<?> collectorExpression : collectorExpressions) {
            collectorExpression.startCollect(lineContext);
        }
    }

    public LineParser.InputType inputType() {
        return lineParser.inputType();
    }

    public Collection<String> allColumns() {
        return lineContext.sourceAsMap().keySet();
    }

    void startWithUri(URI currentUri) {
        lineContext.resetCurrentLineNumber();
        lineContext.currentUri(currentUri);
    }

    @Nullable
    Collection<String> readFirstLine(URI currentUri, InputFormat inputFormat, BufferedReader currentReader) throws IOException {
        return lineParser.readFirstLine(currentUri, inputFormat, currentReader);
    }

    public void process(String line) throws IOException {
        lineContext.incrementCurrentLineNumber();
        lineContext.resetCurrentParsingFailure(); // Reset prev failure if there is any.
        byte[] jsonByteArray = lineParser.getByteArray(line, lineContext.getCurrentLineNumber());
        lineContext.rawSource(jsonByteArray);
    }

    /**
     * Set IO failure. Can be added only once per URI and nullifies
     * `error_count` and `success_count` in summary when encountered.
     */
    public void setUriFailure(String failure) {
        lineContext.setCurrentUriFailure(failure);
    }

    /**
     * Set a non-IO failure. Can be added multiple times per URI and
     * each failure gets traced in `error_count` and  `error` columns
     * in summary.
     */
    public void setParsingFailure(String failure) {
        lineContext.setCurrentParsingFailure(failure);
    }


}
