/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.reference.doc.lucene;

import com.google.common.base.Joiner;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.operation.collect.collectors.CollectorFieldsVisitor;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.Map;

public class DocCollectorExpression extends LuceneCollectorExpression<Map<String, Object>> {

    public static final String COLUMN_NAME = DocSysColumns.DOC.name();

    private CollectorFieldsVisitor visitor;

    public DocCollectorExpression() {
        super(COLUMN_NAME);
    }

    @Override
    public void startCollect(CollectorContext context) {
        context.visitor().required(true);
        this.visitor = context.visitor();
    }

    @Override
    public Map<String, Object> value() {
        return XContentHelper.convertToMap(visitor.source(), false, XContentType.JSON).v2();
    }

    public static LuceneCollectorExpression<?> create(final Reference reference) {
        assert reference.ident().columnIdent().name().equals(DocSysColumns.DOC.name()) :
            "column name must be " + DocSysColumns.DOC.name();
        if (reference.ident().columnIdent().path().size() == 0) {
            return new DocCollectorExpression();
        }

        assert reference.ident().columnIdent().path().size() > 0 : "column's path size must be > 0";
        final String fqn = Joiner.on(".").join(reference.ident().columnIdent().path());
        return new ChildDocCollectorExpression(fqn) {

            @Override
            public void startCollect(CollectorContext context) {
                super.startCollect(context);
            }

            @Override
            public Object value() {
                // need to make sure it has the correct type;
                // for example:
                //      sourceExtractor might read byte as int and
                //      then eq(byte, byte) would get eq(byte, int) and fail
                return reference.valueType().value(sourceLookup.extractValue(fqn));
            }
        };
    }

    public abstract static class ChildDocCollectorExpression<ReturnType> extends
        LuceneCollectorExpression<ReturnType> {

        protected SourceLookup sourceLookup;
        private LeafReaderContext context;

        ChildDocCollectorExpression(String columnName) {
            super(columnName);
        }

        @Override
        public void setNextDocId(int doc) {
            sourceLookup.setSegmentAndDocument(context, doc);
        }

        @Override
        public void setNextReader(LeafReaderContext context) {
            this.context = context;
        }

        @Override
        public void startCollect(CollectorContext context) {
            sourceLookup = context.sourceLookup();
        }
    }
}
