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

import io.crate.metadata.ColumnIdent;
import io.crate.server.xcontent.ParsedXContent;
import io.crate.server.xcontent.XContentHelper;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.xcontent.XContentType;

import org.jetbrains.annotations.Nullable;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.crate.Constants.NO_VALUE_MARKER;

public class LineContext {

    private byte[] rawSource;
    private LinkedHashMap<String, Object> parsedSource;
    private String currentUri;
    private String currentUriFailure;
    private String currentParsingFailure;
    private long currentLineNumber = 0;

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
            return NO_VALUE_MARKER;
        }
        return ColumnIdent.get(parentMap, columnIdent);
    }

    public void rawSource(byte[] bytes) {
        this.rawSource = bytes;
        this.parsedSource = null;
    }

    /**
     * Sets the current URI to the context. This is expected to happen when starting to process a new URI.
     * Any existing URI processing failure must have been consumed already as it will be overwritten/reset.
     */
    public void currentUri(URI currentUri) {
        this.currentUri = currentUri.toString();
        currentUriFailure = null;
        currentParsingFailure = null;
    }

    String currentUri() {
        return currentUri;
    }

    public void setCurrentUriFailure(String failureMessage) {
        currentUriFailure = failureMessage;
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
        return currentUriFailure;
    }

    /**
     * Returns the current parsing (non-IO) failure if any. A NULL value indicates that no failure happened while accessing the URI.
     */
    @Nullable
    String getCurrentParsingFailure() {
        return currentParsingFailure;
    }

    public void resetCurrentLineNumber() {
        currentLineNumber = 0;
    }

    public void incrementCurrentLineNumber() {
        currentLineNumber++;
    }

    public long getCurrentLineNumber() {
        return currentLineNumber;
    }

    public void resetCurrentParsingFailure() {
        currentParsingFailure = null;
    }
}
