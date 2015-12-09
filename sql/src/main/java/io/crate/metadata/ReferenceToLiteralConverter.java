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
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.types.DataType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.util.*;

public class ReferenceToLiteralConverter extends ReplacingSymbolVisitor<ReferenceToLiteralConverter.Context> {

    public static class Context {
        private final Map<ReferenceInfo, InputColumn> referenceInfoInputColumnMap;
        private final BitSet inputIsMap;
        private Object[] values;

        public Context(List<Reference> insertColumns, Collection<ReferenceInfo> allReferencedReferences) {
            referenceInfoInputColumnMap = new HashMap<>(allReferencedReferences.size());
            inputIsMap = new BitSet(insertColumns.size());
            for (ReferenceInfo referenceInfo : allReferencedReferences) {
                int idx = 0;
                for (Reference reference : insertColumns) {
                    if (reference.info().equals(referenceInfo)) {
                        referenceInfoInputColumnMap.put(referenceInfo, new InputColumn(idx, referenceInfo.type()));
                        inputIsMap.set(idx, false);
                        break;
                    } else if (referenceInfo.ident().columnIdent().isChildOf(reference.ident().columnIdent())) {
                        referenceInfoInputColumnMap.put(referenceInfo, new InputColumn(idx, referenceInfo.type()));
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


        public Symbol resolveReferenceValue(Reference reference) {
            assert values != null : "values must be set first";

            InputColumn inputColumn = referenceInfoInputColumnMap.get(reference.info());
            if (inputColumn != null) {
                assert inputColumn.valueType() != null : "expects dataType to be set on InputColumn";
                DataType dataType = inputColumn.valueType();
                Object value;
                if (inputIsMap.get(inputColumn.index())) {
                    ColumnIdent columnIdent = reference.ident().columnIdent().shiftRight();
                    assert columnIdent != null : "shifted ColumnIdent must not be null";

                    //noinspection unchecked
                    value = XContentMapValues.extractValue(
                            columnIdent.fqn(), (Map) values[inputColumn.index()]);
                } else {
                    value = values[inputColumn.index()];
                }
                return Literal.newLiteral(dataType, dataType.value(value));
            }

            DataType dataType = reference.valueType();
            return Literal.newLiteral(dataType, dataType.value(null));
        }

    }

    public ReferenceToLiteralConverter() {
        super(false);
    }

    @Override
    public Symbol visitReference(Reference reference, Context context) {
        return context.resolveReferenceValue(reference);
    }
}
