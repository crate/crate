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

package io.crate.operator.reference.doc;

import org.apache.lucene.index.AtomicReaderContext;
import io.crate.DataType;
import io.crate.exceptions.GroupByOnArrayUnsupportedException;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;

public class LongColumnReference extends FieldCacheExpression<IndexNumericFieldData, Long> {

    private LongValues values;

    public LongColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public Long value() {
        switch (values.setDocument(docId)) {
            case 0:
                return null;
            case 1:
                return values.nextValue();
            default:
                throw new GroupByOnArrayUnsupportedException(columnName());
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        super.setNextReader(context);
        values = indexFieldData.load(context).getLongValues();
    }

    @Override
    public DataType returnType(){
        return DataType.LONG;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof LongColumnReference))
            return false;
        return columnName.equals(((LongColumnReference) obj).columnName);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }
}

