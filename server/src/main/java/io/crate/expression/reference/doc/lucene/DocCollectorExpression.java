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

import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;

import java.io.IOException;
import java.util.Map;

public class DocCollectorExpression extends LuceneCollectorExpression<Map<String, Object>> {

    private SourceLookup sourceLookup;
    private ReaderContext context;

    public DocCollectorExpression() {
        super();
    }

    @Override
    public void startCollect(CollectorContext context) {
        sourceLookup = context.sourceLookup();
    }


    @Override
    public void setNextDocId(int doc) {
        sourceLookup.setSegmentAndDocument(context, doc);
    }

    @Override
    public void setNextReader(ReaderContext context) throws IOException {
        this.context = context;
    }

    @Override
    public Map<String, Object> value() {
        return sourceLookup.sourceAsMap();
    }

    public static LuceneCollectorExpression<?> create(final Reference reference) {
        assert reference.column().name().equals(DocSysColumns.DOC.name()) :
            "column name must be " + DocSysColumns.DOC.name();
        if (reference.column().isTopLevel()) {
            return new DocCollectorExpression();
        }
        return new ChildDocCollectorExpression(reference);
    }

    static final class ChildDocCollectorExpression extends LuceneCollectorExpression<Object> {

        private final Reference ref;
        private SourceLookup sourceLookup;
        private ReaderContext context;

        ChildDocCollectorExpression(Reference ref) {
            this.ref = ref;
        }

        @Override
        public void setNextDocId(int doc) {
            sourceLookup.setSegmentAndDocument(context, doc);
        }

        @Override
        public void setNextReader(ReaderContext context) throws IOException {
            this.context = context;
        }

        @Override
        public void startCollect(CollectorContext context) {
            sourceLookup = context.sourceLookup(ref);
        }

        @Override
        public Object value() {
            // correct type detection is ensured by the source parser
            return sourceLookup.get(ref.column().path());
        }
    }
}
