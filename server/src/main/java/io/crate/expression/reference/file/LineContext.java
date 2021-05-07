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
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Map;

public class LineContext {

    private byte[] rawSource;
    private Map<String, Object> parsedSource;
    private String currentUri;
    private String currentUriFailure;
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
    Map<String, Object> sourceAsMap() {
        if (parsedSource == null) {
            if (rawSource != null) {
                try {
                    parsedSource = XContentHelper.toMap(new BytesArray(rawSource), XContentType.JSON);
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

    /**
     * Sets the current URI to the context. This is expected to happen when starting to process a new URI.
     * Any existing URI processing failure must have been consumed already as it will be overwritten/reset.
     */
    public void currentUri(URI currentUri) {
        this.currentUri = currentUri.toString();
        currentUriFailure = null;
    }

    String currentUri() {
        return currentUri;
    }

    public void setCurrentUriFailure(String failureMessage) {
        currentUriFailure = failureMessage;
    }

    /**
     * Return the current URI failure if any. A NULL value indicates that no failure happened while accessing the URI.
     */
    @Nullable
    String getCurrentUriFailure() {
        return currentUriFailure;
    }

    public void resetCurrentLineNumber() {
        currentLineNumber = 0;
    }

    public void incrementCurrentLineNumber() {
        currentLineNumber++;
    }

    long getCurrentLineNumber() {
        return currentLineNumber;
    }
}
