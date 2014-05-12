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

package io.crate.operation.reference.doc;

import io.crate.exceptions.GroupByOnArrayUnsupportedException;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;

public class BooleanColumnReference extends FieldCacheExpression<IndexFieldData, Boolean> {

    private BytesValues values;
    private static final BytesRef TRUE_BYTESREF = new BytesRef("T");

    public BooleanColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public DataType returnType(){
        return DataTypes.BOOLEAN;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        super.setNextReader(context);
        values = indexFieldData.load(context).getBytesValues(false);
    }

    @Override
    public Boolean value() {
        switch (values.setDocument(docId)) {
            case 0:
                return null;
            case 1:
                return values.nextValue().compareTo(TRUE_BYTESREF) == 0;
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