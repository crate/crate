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

import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.ObjectType;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.List;
import java.util.Map;

public class ReferenceToLiteralConverter extends ReplacingSymbolVisitor<ReferenceToLiteralConverter.Context> {

    private static final ESLogger LOGGER = Loggers.getLogger(ReferenceToLiteralConverter.class);

    public static class Context {
        private final List<Reference> references;
        private final Object[] values;

        public Context(List<Reference> references, Object[] values) {
            this.references = references;
            this.values = values;
        }

        public Symbol resolveReferenceValue(Reference reference) {
            for (int i = 0; i < references.size(); i++) {
                Reference definedReference = references.get(i);
                DataType dataType = null;
                Object value = null;
                ColumnIdent columnIdent = reference.ident().columnIdent();
                List<String> path = columnIdent.path();
                if (definedReference.ident().columnIdent().equals(columnIdent)) {
                    dataType = reference.valueType();
                    value = values[i];
                } else if (definedReference.valueType().id() == ObjectType.ID
                           && path != null
                           && definedReference.ident().columnIdent().name().equals(columnIdent.name())) {
                    dataType = reference.valueType();
                    try {
                        int idx = 0;
                        value = ((Map) values[i]).get(path.get(idx++));
                        while (idx < path.size()) {
                            value = ((Map) value).get(path.get(idx));
                        }
                    } catch (Exception e) {
                        LOGGER.error("Exception occurred while resolving referenced column value", e);
                        break;
                    }
                }

                if (dataType != null) {
                    return Literal.newLiteral(dataType, dataType.value(value));
                }
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
