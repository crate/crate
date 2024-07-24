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
import java.io.UncheckedIOException;

import org.apache.lucene.util.IORunnable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * A TranslogWriter implementation that writes the transaction log entry as
 * a json map
 */
public class XContentTranslogWriter implements TranslogWriter {

    private final XContentBuilder builder;
    private final BytesStreamOutput output = new BytesStreamOutput();

    public XContentTranslogWriter() {
        try {
            this.builder = XContentFactory.json(output);
            this.builder.startObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void uncheck(IORunnable runnable) {
        try {
            runnable.run();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void startArray() {
        uncheck(builder::startArray);
    }

    @Override
    public void endArray() {
        uncheck(builder::endArray);
    }

    @Override
    public void startObject() {
        uncheck(builder::startObject);
    }

    @Override
    public void endObject() {
        uncheck(builder::endObject);
    }

    @Override
    public void writeNull() {
        uncheck(builder::nullValue);
    }

    @Override
    public void writeFieldName(String fieldName) {
        uncheck(() -> builder.field(fieldName));
    }

    @Override
    public void writeValue(Object value) {
        uncheck(() -> builder.value(value));
    }

    @Override
    public BytesReference bytes() {
        uncheck(builder::endObject);
        builder.close();
        return output.bytes();
    }
}
