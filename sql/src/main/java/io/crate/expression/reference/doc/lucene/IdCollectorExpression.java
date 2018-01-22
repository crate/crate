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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;

public class IdCollectorExpression extends FieldCacheExpression<IndexOrdinalsFieldData, BytesRef> {

    public static final String COLUMN_NAME = DocSysColumns.ID.name();

    private SortedSetDocValues values;
    private BytesRef value;

    public IdCollectorExpression(MappedFieldType mappedFieldType) {
        super(COLUMN_NAME, mappedFieldType);
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        super.setNextDocId(docId);
        if (values.advanceExact(docId)) {
            value = BytesRef.deepCopyOf(values.lookupOrd(values.nextOrd()));
        } else {
            value = null;
        }
    }

    @Override
    public BytesRef value() {
        return value;
    }

    @Override
    public void setNextReader(LeafReaderContext context) throws IOException {
        values = indexFieldData.load(context).getOrdinalsValues();
    }
}
