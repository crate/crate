/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.sort;

import io.crate.data.Input;
import io.crate.execution.engine.collect.collectors.CollectorFieldsVisitor;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.types.DataType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafFieldComparator;

import java.io.IOException;

class FieldsVisitorInputFieldComparator extends InputFieldComparator {

    private final CollectorFieldsVisitor fieldsVisitor;
    private IndexReader currentReader;

    FieldsVisitorInputFieldComparator(int numHits,
                                      CollectorFieldsVisitor fieldsVisitor,
                                      Iterable<? extends LuceneCollectorExpression<?>> collectorExpressions,
                                      Input input,
                                      DataType valueType,
                                      Object missingValue) {
        super(numHits, collectorExpressions, input, valueType, missingValue);
        this.fieldsVisitor = fieldsVisitor;
        assert fieldsVisitor.required() : "Use InputFieldComparator if FieldsVisitor is not required";
    }

    @Override
    public int compareBottom(int doc) throws IOException {
        setFieldsVisitor(doc);
        return super.compareBottom(doc);
    }

    @Override
    public int compareTop(int doc) throws IOException {
        setFieldsVisitor(doc);
        return super.compareTop(doc);
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
        setFieldsVisitor(doc);
        super.copy(slot, doc);
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
        currentReader = context.reader();
        return super.getLeafComparator(context);
    }

    private void setFieldsVisitor(int doc) {
        fieldsVisitor.reset();
        try {
            currentReader.document(doc, fieldsVisitor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
