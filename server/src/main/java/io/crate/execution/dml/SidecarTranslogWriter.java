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
 * Wraps a TranslogWriter and makes any entries written after wrapping available
 * as a separate BytesReference
 */
class SidecarTranslogWriter implements TranslogWriter {

    private final TranslogWriter inner;
    private final TranslogWriter sidecar;

    SidecarTranslogWriter(TranslogWriter inner, String oid) {
        this.inner = inner;
        this.sidecar = new XContentTranslogWriter();
        this.sidecar.writeFieldName(oid);
    }

    @Override
    public void startArray() {
        inner.startArray();
        sidecar.startArray();
    }

    @Override
    public void endArray() {
        inner.endArray();
        sidecar.endArray();
    }

    @Override
    public void startObject() {
        inner.startObject();
        sidecar.startObject();
    }

    @Override
    public void endObject() {
        inner.endObject();
        sidecar.endObject();
    }

    @Override
    public void writeFieldName(String fieldName) {
        inner.writeFieldName(fieldName);
        sidecar.writeFieldName(fieldName);
    }

    @Override
    public void writeNull() {
        inner.writeNull();
        sidecar.writeNull();
    }

    @Override
    public void writeValue(Object value) {
        inner.writeValue(value);
        sidecar.writeValue(value);
    }

    @Override
    public BytesReference bytes() {
        return sidecar.bytes();
    }
}
