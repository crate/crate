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

package io.crate.expression.reference.doc.lucene;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;

import io.crate.execution.engine.fetch.ReaderContext;

public class FloatVectorColumnReference extends LuceneCollectorExpression<float[]> {

    private final String fqn;
    private BinaryDocValues docValues;
    private int doc;

    public FloatVectorColumnReference(String fqn) {
        this.fqn = fqn;
    }

    @Override
    public float[] value() {
        try {
            if (docValues != null && docValues.advanceExact(doc)) {
                BytesRef bytesRef = docValues.binaryValue();
                float[] values = new float[bytesRef.length / Float.BYTES];
                ByteBuffer buffer = ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                buffer.asFloatBuffer().get(values);
                return values;
            }
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setNextReader(ReaderContext context) throws IOException {
        docValues = context.reader().getBinaryDocValues(fqn);
    }

    @Override
    public void setNextDocId(int doc) {
        this.doc = doc;
    }
}
