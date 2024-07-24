/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.dml;

import org.elasticsearch.common.bytes.BytesReference;

/**
 * Builds a transaction log entry for an indexed document
 */
public interface TranslogWriter {

    /** Start writing an array of values */
    void startArray();

    /** Finish writing an array of values */
    void endArray();

    /** Start writing a key-value object */
    void startObject();

    /** Finish writing a key-value object */
    void endObject();

    /** Write a field name */
    void writeFieldName(String fieldName);

    /** Write a null field value */
    void writeNull();

    /** Write a non-null field value */
    void writeValue(Object value);

    /** Write an array of null values */
    default void writeNullArray(int size) {
        startArray();
        for (int i = 0; i < size; i++) {
            writeNull();
        }
        endArray();
    }

    /**
     * Return a byte array representation of the transaction log entry
     * <p/>
     * Once this method has been called, no other methods should be called
     * on the TranslogWriter
     */
    BytesReference bytes();
}
