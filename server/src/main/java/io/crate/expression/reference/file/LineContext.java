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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.xcontent.XContentType;
import org.jetbrains.annotations.Nullable;

import io.crate.execution.engine.collect.files.FileReadingIterator.LineCursor;
import io.crate.metadata.ColumnIdent;
import io.crate.server.xcontent.ParsedXContent;
import io.crate.server.xcontent.XContentHelper;

public class LineContext {

    private final LineCursor cursor;

    private byte[] rawSource;
    private LinkedHashMap<String, Object> parsedSource;
    private String currentParsingFailure;

    public LineContext(LineCursor cursor) {
        this.cursor = cursor;
    }

    @Nullable
    String sourceAsString() {
        if (rawSource != null) {
            char[] chars = new char[rawSource.length];
            int len = UnicodeUtil.UTF8toUTF16(rawSource, 0, rawSource.length, chars);
            return new String(chars, 0, len);
        }
        return null;
    }

    @Nullable
    LinkedHashMap<String, Object> sourceAsMap() {
        if (parsedSource == null) {
            if (rawSource != null) {
                try {
                    // preserve the order of the rawSource
                    ParsedXContent parsedXContent = XContentHelper.convertToMap(new BytesArray(rawSource), true, XContentType.JSON);
                    parsedSource = (LinkedHashMap<String, Object>) parsedXContent.map();
                } catch (ElasticsearchParseException | NotXContentException e) {
                    throw new RuntimeException("JSON parser error: " + e.getMessage(), e);
                }
            }
        }
        return parsedSource;
    }

    public Object get(ColumnIdent columnIdent) {
        Map<String, Object> parentMap = sourceAsMap();
        if (parentMap == null) {
            return null;
        }
        return ColumnIdent.get(parentMap, columnIdent);
    }

    public void rawSource(byte[] bytes) {
        this.rawSource = bytes;
        this.parsedSource = null;
    }

    String currentUri() {
        return cursor.uri().toString();
    }


    /**
     * Sets the current parsing failure.
     */
    public void setCurrentParsingFailure(String failureMessage) {
        currentParsingFailure = failureMessage;
    }

    /**
     * Returns the current URI failure if any. A NULL value indicates that no failure happened while accessing the URI.
     */
    @Nullable
    String getCurrentUriFailure() {
        IOException failure = cursor.failure();
        return failure == null ? null : failure.getMessage();
    }

    /**
     * Returns the current parsing (non-IO) failure if any. A NULL value indicates that no failure happened while accessing the URI.
     */
    @Nullable
    String getCurrentParsingFailure() {
        return currentParsingFailure;
    }

    public long getCurrentLineNumber() {
        return cursor.lineNumber();
    }

    public void resetCurrentParsingFailure() {
        currentParsingFailure = null;
    }
}
