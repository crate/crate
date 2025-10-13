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

import java.io.IOException;

import org.elasticsearch.common.bytes.BytesReference;

/**
 * Builds a translog entry for an indexed row
 */
public interface TranslogWriter {

    /** Start writing an array of values */
    void startArray() throws IOException;

    /** Finish writing an array of values */
    void endArray() throws IOException;

    /** Start writing a key-value object */
    void startObject() throws IOException;

    /** Finish writing a key-value object */
    void endObject() throws IOException;

    /** Write a field name */
    void writeFieldName(String fieldName) throws IOException;

    /** Write a null field value */
    void writeNull() throws IOException;

    /** Write a non-null field value */
    void writeValue(Object value) throws IOException;

    void writeValue(String value) throws IOException;

    void writeValue(int value) throws IOException;

    void writeValue(long value) throws IOException;

    void writeValue(float value) throws IOException;

    void writeValue(double value) throws IOException;

    /**
     * Return a byte array representation of the transaction log entry
     * <p/>
     * Once this method has been called, no other methods should be called
     * on the TranslogWriter
     */
    BytesReference bytes() throws IOException;

    /**
     * Create a TranslogWriter that will ignore any write methods, and return
     * the given source as its final BytesReference
     */
    static TranslogWriter wrapBytes(BytesReference source) {
        return new TranslogWriter() {
            @Override
            public void startArray() {

            }

            @Override
            public void endArray() {

            }

            @Override
            public void startObject() {

            }

            @Override
            public void endObject() {

            }

            @Override
            public void writeFieldName(String fieldName) {

            }

            @Override
            public void writeNull() {

            }

            @Override
            public void writeValue(Object value) {
            }

            @Override
            public void writeValue(String value) {
            }

            @Override
            public void writeValue(int value) {
            }

            @Override
            public void writeValue(long value) {
            }

            @Override
            public void writeValue(float value) {
            }

            @Override
            public void writeValue(double value) {
            }

            @Override
            public BytesReference bytes() {
                return source;
            }
        };
    }
}
