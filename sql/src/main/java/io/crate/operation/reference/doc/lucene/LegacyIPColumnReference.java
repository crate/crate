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

import io.crate.exceptions.GroupByOnArrayUnsupportedException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.LegacyIpFieldMapper;

import java.io.IOException;

public class LegacyIPColumnReference extends LuceneCollectorExpression<BytesRef> {

    private SortedNumericDocValues values;
    private BytesRef value;

    public LegacyIPColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public BytesRef value() {
        return value;
    }

    @Override
    public void setNextDocId(int docId) {
        super.setNextDocId(docId);
        values.setDocument(docId);
        switch (values.count()) {
            case 0:
                value = null;
                break;
            case 1:
                value = new BytesRef(LegacyIpFieldMapper.longToIp(values.valueAt(0)));
                break;
            default:
                throw new GroupByOnArrayUnsupportedException(columnName);
        }
    }

    @Override
    public void setNextReader(LeafReaderContext context) throws IOException {
        super.setNextReader(context);
        values = DocValues.getSortedNumeric(context.reader(), columnName);
    }
}
