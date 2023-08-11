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
import io.crate.server.xcontent.ParsedXContent;
import io.crate.server.xcontent.XContentHelper;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.xcontent.XContentType;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class JsonColumnExtractingExpression extends FileParsingExpression {

    private final ColumnIdent columnIdent;

    public JsonColumnExtractingExpression(RelationName tableIdent,
                                          List<Reference> targetColumns,
                                          ColumnIdent columnIdent,
                                          RowContext rowContext) {
        super(tableIdent, targetColumns, rowContext);
        this.columnIdent = columnIdent;
    }

    @Override
    public Object value() {
        if (rowContext.rowAsMap() == null) {
            rowContext.rowAsMap(parseRow());
        }
        return ValueExtractors.fromMap(rowContext.rowAsMap(), columnIdent);
    }

    @Override
    Map<String, Object> parseRow() {
        Object[] values = row.materialize();
        String line = (String) values[0];
        if (line != null) {
            Long lineNumber = (Long) values[1];

            try {
                // preserve the order of the rawSource
                ParsedXContent parsedXContent = XContentHelper.convertToMap(
                    new BytesArray(line.getBytes(StandardCharsets.UTF_8)),
                    true, XContentType.JSON
                );
                var rowAsMap = parsedXContent.map();
                if (lineNumber == 1) {
                    finalizeTargetColumns(rowAsMap.keySet());
                }
                return rowAsMap;
            } catch (ElasticsearchParseException | NotXContentException e) {
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
