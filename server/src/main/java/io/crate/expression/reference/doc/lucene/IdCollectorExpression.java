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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.index.fieldvisitor.IDVisitor;

import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.metadata.doc.SysColumns;

public abstract class IdCollectorExpression extends LuceneCollectorExpression<BytesRef> {

    public static final Version STORED_AS_BINARY_VERSION = Version.V_6_0_0;

    public static IdCollectorExpression forVersion(Version version) {
        if (version.before(STORED_AS_BINARY_VERSION)) {
            return new StoredIdCollectorExpression();
        } else {
            return new BinaryIdCollectorExpression();
        }
    }

    protected int docId;

    @Override
    public void setNextDocId(int docId) {
        this.docId = docId;
    }

    private static class StoredIdCollectorExpression extends IdCollectorExpression {

        private final IDVisitor visitor = new IDVisitor(SysColumns.ID.COLUMN.name());
        private ReaderContext context;

        @Override
        public BytesRef value() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setNextReader(ReaderContext context) throws IOException {
            this.context = context;
        }
    }

    private static class BinaryIdCollectorExpression extends IdCollectorExpression {

        private BinaryDocValues values;

        @Override
        public BytesRef value() {
            try {
                if (this.values.advanceExact(this.docId)) {
                    return BytesRef.deepCopyOf(this.values.binaryValue());

                } else {
                    throw new IllegalStateException("No binary id for document " + this.docId);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void setNextReader(ReaderContext context) throws IOException {
            this.values = DocValues.getBinary(context.reader(), SysColumns.Names.ID);
        }
    }
}
