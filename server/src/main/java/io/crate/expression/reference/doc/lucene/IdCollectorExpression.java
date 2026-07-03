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
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.index.fieldvisitor.IDVisitor;
import org.elasticsearch.index.mapper.Uid;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.metadata.doc.SysColumns;

/**
 * CrateDB 6.0+ stores id in BinaryDocValues.
 * This class is not relying on version_created and instead adapts based on actual state.
 */
public class IdCollectorExpression extends LuceneCollectorExpression<String> {

    public static final Version STORED_AS_BINARY_VERSION = Version.V_6_0_0;

    protected int docId;

    private final IDVisitor visitor = new IDVisitor(SysColumns.ID.COLUMN.name());
    private ReaderContext context;
    private BinaryDocValues binaryValues;


    @VisibleForTesting
    BinaryDocValues binaryValues() {
        return binaryValues;
    }

    @Override
    public void setNextDocId(int docId) {
        this.docId = docId;
    }

    @Override
    public String value() {
        try {
            if (binaryValues != null) {
                if (binaryValues.advanceExact(this.docId)) {
                    BytesRef bytes = this.binaryValues.binaryValue();
                    return Uid.decodeId(bytes.bytes, bytes.offset, bytes.length);
                } else {
                    throw new IllegalStateException("No binary id for document " + this.docId);
                }
            } else {
                visitor.setCanStop(false);
                context.visitDocument(docId, visitor);
                return visitor.getId();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setNextReader(ReaderContext context) throws IOException {
        this.context = context;
        var reader = this.context.reader();
        FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(SysColumns.Names.ID);
        if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.BINARY) {
            this.binaryValues = DocValues.getBinary(reader, SysColumns.Names.ID);
        }
    }
}
