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
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.ValueExtractors;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.collect.files.CSVLineParser;
import io.crate.types.DataType;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

public class CsvColumnExtractingExpression implements CollectExpression<Row, Object> {

    private final CSVLineParser csvLineParser;
    private final ColumnIdent columnIdent;
    private final DataType<?> type;

    private Map<String, Object> rowAsMap;

    public CsvColumnExtractingExpression(ColumnIdent columnIdent,
                                         DataType<?> type,
                                         CSVLineParser csvLineParser) {
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
        String line = (String) row.materialize()[0];
        if (line != null) {
            // TODO: Share some context (and rowAsMap) for all expressions.
            // Reset it after consuming each row, similar to LineContext.startCollect
            try {
                rowAsMap = csvLineParser.rowAsMapWithoutHeader(line);
            } catch (IOException e) {
                // TODO: Enrich transformed row with IOFailure for RETURN SUMMARY case. Used to be done in FileReadingIterator.
                throw new RuntimeException("JSON parser error: " + e.getMessage(), e);
            } catch (Exception e) {
                // TODO: Enrich transformed row with parsingFailure for RETURN SUMMARY case. Used to be done in FileReadingIterator.
            }
        }
    }
}
