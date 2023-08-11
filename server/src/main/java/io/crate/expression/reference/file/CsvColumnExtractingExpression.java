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

import io.crate.expression.ValueExtractors;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.operation.collect.files.CSVLineParser;
import io.crate.types.DataType;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class CsvColumnExtractingExpression extends FileParsingExpression {
    private final CSVLineParser csvLineParser;
    private final ColumnIdent columnIdent;
    private final DataType<?> type;

    public CsvColumnExtractingExpression(RelationName tableIdent,
                                         List<Reference> targetColumns,
                                         ColumnIdent columnIdent,
                                         RowContext rowContext,
                                         DataType<?> type,
                                         CSVLineParser csvLineParser) {
        super(tableIdent, targetColumns, rowContext);
        this.columnIdent = columnIdent;
        this.type = type;
        this.csvLineParser = csvLineParser;
    }

    @Override
    public Object value() {
        if (rowContext.rowAsMap() == null) {
            rowContext.rowAsMap(parseRow());
        }
        return type.implicitCast(ValueExtractors.fromMap(rowContext.rowAsMap(), columnIdent));
    }


    @Override
    Map<String, Object> parseRow() {
        Object[] values = row.materialize();
        String line = (String) values[0];
        if (line != null) {
            try {
                Long lineNumber = (Long) values[1];
                if (csvLineParser.header()) {
                    if (lineNumber == 1) {
                        String[] headerColumns = csvLineParser.parseHeader(line);
                        finalizeTargetColumns(Arrays.asList(headerColumns));
                        return null;
                    } else {
                        return csvLineParser.rowAsMapWithHeader(line);
                    }
                } else {
                    return csvLineParser.rowAsMapWithoutHeader(line);
                }

            } catch (Exception e) {
                return null;
                // TODO: Enrich transformed row with parsingFailure for RETURN SUMMARY case. Used to be done in FileReadingIterator.
            }
        } else {
            throw new IllegalStateException(
                String.format(Locale.ENGLISH, "Cannot read column %s from non-existent row", columnIdent.fqn())
            );
        }
    }
}
