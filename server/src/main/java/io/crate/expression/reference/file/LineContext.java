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

import javax.annotation.Nullable;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;

public class LineContext {

    private LinkedHashMap<String, Object> valuesByColumnName;
    private String currentUri;
    private String currentUriFailure;
    private String currentParsingFailure;
    private long currentLineNumber = 0;

    @Nullable
    String sourceAsString() {
        if (valuesByColumnName != null) {
            return valuesByColumnName.toString();
            // TODO: Maybe replace with new String(valuesByColumnName.toString(), Charset.forName("UTF-16")) to align with the prev version?
        }
        return null;
    }

    @Nullable
    LinkedHashMap<String, Object> sourceAsMap() {
        return valuesByColumnName;
    }

    public Object get(ColumnIdent columnIdent) {
        // TODO: check, maybe need to use sqlFqn, depends on targetColumns format
        return valuesByColumnName.get(columnIdent.fqn());
    }

    /**
     * Build a map (name -> value) so that we can quickly get column value by name.
     * k-th element of the values contains value of the k-th target column.
     */
    public void values(Object[] values, List<String> targetColumns) {
        assert targetColumns.size() == values.length : "target columns and their values must have the same size";
        for (int i = 0; i < targetColumns.size(); i++) {
            valuesByColumnName.put(targetColumns.get(i), values[i]);
        }
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
