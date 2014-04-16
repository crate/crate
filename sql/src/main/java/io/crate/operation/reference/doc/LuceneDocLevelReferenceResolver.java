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

import io.crate.exceptions.CrateException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.ReferenceInfo;
import io.crate.operation.reference.DocLevelReferenceResolver;
import io.crate.planner.RowGranularity;

public class LuceneDocLevelReferenceResolver implements DocLevelReferenceResolver<LuceneCollectorExpression<?>> {

    public static final LuceneDocLevelReferenceResolver INSTANCE = new LuceneDocLevelReferenceResolver();

    private LuceneDocLevelReferenceResolver() {
    }

    @Override
    public LuceneCollectorExpression<?> getImplementation(ReferenceInfo referenceInfo) {
        assert referenceInfo.granularity() == RowGranularity.DOC;

        if (RawCollectorExpression.COLUMN_NAME.equals(referenceInfo.ident().columnIdent().name())){
            if (referenceInfo.ident().columnIdent().isColumn()){
                return new RawCollectorExpression();
            } else {
                // TODO: implement an Object source expression which may support subscripts
                throw new UnsupportedFeatureException(
                        String.format("_source expression does not support subscripts %",
                        referenceInfo.ident().columnIdent().fqn()));
            }
        } else if (DocCollectorExpression.COLUMN_NAME.equals(referenceInfo.ident().columnIdent().name())) {
            return DocCollectorExpression.create(referenceInfo);
        }

        String colName = referenceInfo.ident().columnIdent().fqn();

        switch (referenceInfo.type()) {
            case BYTE:
                return new ByteColumnReference(colName);
            case SHORT:
                return new ShortColumnReference(colName);
            case IP:
            case STRING:
                return new BytesRefColumnReference(colName);
            case DOUBLE:
                return new DoubleColumnReference(colName);
            case BOOLEAN:
                return new BooleanColumnReference(colName);
            case OBJECT:
                return new ObjectColumnReference(colName);
            case FLOAT:
                return new FloatColumnReference(colName);
            case LONG:
            case TIMESTAMP:
                return new LongColumnReference(colName);
            case INTEGER:
                return new IntegerColumnReference(colName);
            default:
                throw new CrateException(String.format("unsupported type '%s'", referenceInfo.type().getName()));
        }
    }
}
