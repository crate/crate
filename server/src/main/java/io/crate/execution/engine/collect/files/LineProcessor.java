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

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;

public final class LineProcessor {

    private final LineContext lineContext = new LineContext();
    private final LineParser lineParser;

    public LineProcessor(CopyFromParserProperties parserProperties) {
        lineParser = new LineParser(parserProperties);
    }

    public void startCollect(Iterable<LineCollectorExpression<?>> collectorExpressions) {
        for (LineCollectorExpression<?> collectorExpression : collectorExpressions) {
            collectorExpression.startCollect(lineContext);
        }
    }

    void startWithUri(URI currentUri) {
        lineContext.resetCurrentLineNumber();
        lineContext.currentUri(currentUri);
    }

    void readFirstLine(URI currentUri, InputFormat inputFormat, BufferedReader currentReader) throws IOException {
        lineParser.readFirstLine(currentUri, inputFormat, currentReader);
    }

    public void process(String line) throws IOException {
        lineContext.incrementCurrentLineNumber();
        byte[] jsonByteArray = lineParser.getByteArray(line);
        lineContext.rawSource(jsonByteArray);
    }

    public void setFailure(String failure) {
        lineContext.setCurrentUriFailure(failure);
    }
}
