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

package io.crate.operation.reference.doc.lucene;

import io.crate.exceptions.GroupByOnArrayUnsupportedException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

public class BooleanColumnReference extends FieldCacheExpression<IndexFieldData, Boolean> {

    private static final BytesRef TRUE_BYTESREF = new BytesRef("1");
    private SortedBinaryDocValues values;

    public BooleanColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public void setNextReader(LeafReaderContext context) {
        super.setNextReader(context);
        values = indexFieldData.load(context).getBytesValues();
    }

    @Override
    public void setNextDocId(int docId) {
        super.setNextDocId(docId);
        values.setDocument(docId);
    }

    @Override
    public Boolean value() {
        switch (values.count()) {
            case 0:
                return null;
            case 1:
                return values.valueAt(0).compareTo(TRUE_BYTESREF) == 0;
            default:
                throw new GroupByOnArrayUnsupportedException(columnName());
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof BooleanColumnReference))
            return false;
        return columnName.equals(((BooleanColumnReference) obj).columnName);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }
}
