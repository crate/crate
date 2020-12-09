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

import io.crate.execution.engine.fetch.FieldReader;
import io.crate.metadata.doc.DocSysColumns;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.index.fieldvisitor.IDVisitor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;
import java.util.function.Supplier;

public final class IdCollectorExpression extends LuceneCollectorExpression<String> {

    private final IDVisitor visitor = new IDVisitor(DocSysColumns.ID.name());
    private  boolean isSequental;
    private LeafReaderContext context;
    private int docId;

    public IdCollectorExpression() {
    }

    @Override
    public void setNextDocId(int docId) {
        this.docId = docId;
    }

    @Override
    public String value() {
        try {
            visitor.setCanStop(false);
            if (isSequental) {
                FieldReader.getSequentialFieldReaderIfAvailable(context).accept(docId, visitor);
            } else {
                FieldReader.getFieldReader(context).accept(docId, visitor);
            }
            return visitor.getId();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setNextReader(LeafReaderContext context, boolean isSequental) throws IOException {
        this.context = context;
        this.isSequental = isSequental;
    }
}
