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

package io.crate.operation.reference.doc;

import com.google.common.base.Joiner;
import io.crate.DataType;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.operation.collect.LuceneDocCollector;
import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.Map;

public class DocCollectorExpression extends
        LuceneCollectorExpression<Map<String, Object>> implements ColumnReferenceExpression {

    public static final String COLUMN_NAME = DocSysColumns.DOC.name();

    private LuceneDocCollector.CollectorFieldsVisitor visitor;

    @Override
    public void startCollect(CollectorContext context) {
        context.visitor().required(true);
        this.visitor = context.visitor();
    }

    @Override
    public DataType returnType() {
        return DataType.OBJECT;
    }

    @Override
    public Map<String, Object> value() {
        return XContentHelper.convertToMap(visitor.source(), false).v2();
    }

    @Override
    public String columnName() {
        return COLUMN_NAME;
    }

    @SuppressWarnings("unchecked")
    public static <T> LuceneCollectorExpression<T> create(final ReferenceInfo referenceInfo) {
        assert referenceInfo.ident().columnIdent().name().equals(DocSysColumns.DOC.name());
        if (referenceInfo.ident().columnIdent().path().size() == 0) {
            return (LuceneCollectorExpression) new DocCollectorExpression();
        }

        assert referenceInfo.ident().columnIdent().path().size() > 0;
        final String fqn = Joiner.on(".").join(referenceInfo.ident().columnIdent().path());
        return new ChildDocCollectorExpression<T>() {

            @Override
            public void startCollect(CollectorContext context) {
                super.startCollect(context);
            }

            @Override
            public DataType returnType() {
                return referenceInfo.type();
            }

            @Override
            public T value() {
                return (T)sourceLookup.extractValue(fqn);
            }

            @Override
            public String columnName() {
                return referenceInfo.ident().columnIdent().path().get(0);
            }
        };
    }

    abstract static class ChildDocCollectorExpression<ReturnType> extends
            LuceneCollectorExpression<ReturnType> implements ColumnReferenceExpression {

        protected SourceLookup sourceLookup;

        @Override
        public void setNextDocId(int doc) {
            sourceLookup.setNextDocId(doc);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) {
            sourceLookup.setNextReader(context);
        }

        @Override
        public void startCollect(CollectorContext context) {
            sourceLookup = context.searchContext().lookup().source();
        }
    }
}
