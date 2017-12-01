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

package io.crate.metadata;

import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.types.DataType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ReferenceToLiteralConverter implements Function<Reference, Symbol> {

    private final Map<Reference, InputColumn> referenceInputColumnMap;
    private final BitSet inputIsMap;
    private Object[] values;

    public ReferenceToLiteralConverter(List<Reference> insertColumns, Collection<Reference> allReferencedReferences) {
        referenceInputColumnMap = new HashMap<>(allReferencedReferences.size());
        inputIsMap = new BitSet(insertColumns.size());
        for (Reference reference : allReferencedReferences) {
            int idx = 0;
            for (Reference insertColumn : insertColumns) {
                if (insertColumn.equals(reference)) {
                    referenceInputColumnMap.put(reference, new InputColumn(idx, reference.valueType()));
                    inputIsMap.set(idx, false);
                    break;
                } else if (reference.column().isChildOf(insertColumn.column())) {
                    referenceInputColumnMap.put(reference, new InputColumn(idx, reference.valueType()));
                    inputIsMap.set(idx, true);
                    break;
                }
                idx++;
            }
        }
    }

    public void values(Object[] values) {
        this.values = values;
    }

    private Symbol resolveReferenceValue(Reference reference) {
        assert values != null : "values must be set first";

        InputColumn inputColumn = referenceInputColumnMap.get(reference);
        if (inputColumn != null) {
            assert inputColumn.valueType() != null : "expects dataType to be set on InputColumn";
            DataType dataType = inputColumn.valueType();
            Object value;
            if (inputIsMap.get(inputColumn.index())) {
                ColumnIdent columnIdent = reference.column().shiftRight();
                assert columnIdent != null : "shifted ColumnIdent must not be null";

                //noinspection unchecked
                value = XContentMapValues.extractValue(
                    columnIdent.fqn(), (Map) values[inputColumn.index()]);
            } else {
                value = values[inputColumn.index()];
            }
            return Literal.of(dataType, dataType.value(value));
        }

        DataType dataType = reference.valueType();
        return Literal.of(dataType, dataType.value(null));
    }

    @Override
    public Symbol apply(Reference reference) {
        return resolveReferenceValue(reference);
    }
}
