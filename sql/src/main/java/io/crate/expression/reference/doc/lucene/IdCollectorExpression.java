/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.metadata.doc.DocSysColumns;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public final class IdCollectorExpression extends LuceneCollectorExpression<BytesRef> {

    public static final String COLUMN_NAME = DocSysColumns.ID.name();
    private final IDVisitor visitor = new IDVisitor();
    private LeafReader reader;

    public IdCollectorExpression() {
        super(COLUMN_NAME);
    }

    @Override
    public void setNextDocId(int docId) {
        visitor.canStop = false;
        try {
            reader.document(docId, visitor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BytesRef value() {
        return visitor.id;
    }

    @Override
    public void setNextReader(LeafReaderContext context) {
        reader = context.reader();
    }


    private static class IDVisitor extends StoredFieldVisitor {

        private boolean canStop = false;
        private BytesRef id;

        @Override
        public Status needsField(FieldInfo fieldInfo) {
            if (canStop) {
                return Status.STOP;
            }
            if (COLUMN_NAME.equals(fieldInfo.name)) {
                canStop = true;
                return Status.YES;
            }
            return Status.NO;
        }

        @Override
        public void stringField(FieldInfo fieldInfo, byte[] bytes) {
            assert COLUMN_NAME.equals(fieldInfo.name) : "binaryField must only be called for id";
            id = new BytesRef(bytes);
        }
    }
}
