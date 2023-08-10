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

package io.crate.expression.reference.file;

import io.crate.data.Row;
import io.crate.execution.dsl.projection.FileParsingProjection;
import io.crate.expression.ValueExtractors;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.operation.collect.files.CSVLineParser;
import io.crate.types.DataType;

import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

public class CsvColumnExtractingExpression extends FileParsingExpression {
    private final CSVLineParser csvLineParser;
    private final ColumnIdent columnIdent;
    private final DataType<?> type;

    private Map<String, Object> rowAsMap;

    public CsvColumnExtractingExpression(ColumnIdent columnIdent,
                                         DataType<?> type,
                                         CSVLineParser csvLineParser,
                                         FileParsingProjection fileParsingProjection) {
        super(fileParsingProjection);
        this.columnIdent = columnIdent;
        this.type = type;
        this.csvLineParser = csvLineParser;
    }

    @Override
    public Object value() {
        if (rowAsMap == null) {
            throw new IllegalStateException(
                String.format(Locale.ENGLISH, "Cannot read column %s from non-existent row", columnIdent.fqn())
            );
        }
        return type.implicitCast(ValueExtractors.fromMap(rowAsMap, columnIdent));
    }

    @Override
    public void setNextRow(Row row) {
        Object[] values = row.materialize();
        String line = (String) values[0];
        if (line != null) {
            Long lineNumber = (Long) values[1];
            try {
                if (fileParsingProjection.copyFromParserProperties().fileHeader()) {
                    if (lineNumber == 1) {
                        String[] headerColumns = csvLineParser.parseHeader(line);
                        finalizeTargetColumns(Arrays.asList(headerColumns));
                    } else {
                        rowAsMap = csvLineParser.rowAsMapWithHeader(line);
                    }
                } else {
                    // TODO: Share some context (and rowAsMap) for all expressions.
                    // Reset it after consuming each row, similar to LineContext.startCollect
                    rowAsMap = csvLineParser.rowAsMapWithoutHeader(line);
                }
            } catch (Exception e) {
                // parseHeader can throw IOException but we treat it as a parsing failure because data is already read.
                // Only generic IOExceptions happening on file reading close a reader and reported in uriFailure column.
                // TODO: Enrich transformed row with parsingFailure for RETURN SUMMARY case. Used to be done in FileReadingIterator.
            }
        }
    }
}
