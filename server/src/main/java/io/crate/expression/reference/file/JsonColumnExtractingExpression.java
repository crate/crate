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
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.ValueExtractors;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.server.xcontent.ParsedXContent;
import io.crate.server.xcontent.XContentHelper;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.xcontent.XContentType;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

public class JsonColumnExtractingExpression extends FileParsingExpression {

    private final ColumnIdent columnIdent;

    private LinkedHashMap<String, Object> rowAsMap;

    public JsonColumnExtractingExpression(ColumnIdent columnIdent, FileParsingProjection fileParsingProjection) {
        super(fileParsingProjection);
        this.columnIdent = columnIdent;
    }

    @Override
    public Object value() {
        if (rowAsMap == null) {
            throw new IllegalStateException(
                String.format(Locale.ENGLISH, "Cannot read column %s from non-existent row", columnIdent.fqn())
            );
        }
        return ValueExtractors.fromMap(rowAsMap, columnIdent);
    }

    @Override
    public void setNextRow(Row row) {
        Object[] values = row.materialize();
        String line = (String) values[0];
        if (line != null) {
            Long lineNumber = (Long) values[1];
            // TODO: Share some context (and rowAsMap) for all expressions.
            // Reset it after consuming each row, similar to LineContext.startCollect
            try {
                // preserve the order of the rawSource
                ParsedXContent parsedXContent = XContentHelper.convertToMap(
                    new BytesArray(line.getBytes(StandardCharsets.UTF_8)),
                    true, XContentType.JSON
                );
                rowAsMap = (LinkedHashMap<String, Object>) parsedXContent.map();
                if (lineNumber == 1) {
                    finalizeTargetColumns(rowAsMap.keySet());
                }
            } catch (ElasticsearchParseException | NotXContentException e) {
                // TODO: Enrich transformed row with parsingFailure for RETURN SUMMARY case. Used to be done in FileReadingIterator.
                throw new RuntimeException("JSON parser error: " + e.getMessage(), e);
            }
        }
    }
}
