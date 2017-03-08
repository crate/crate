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

import io.crate.exceptions.UnhandledServerException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.lucene.FieldTypeLookup;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocReferenceConverter;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.operation.reference.ReferenceResolver;
import io.crate.types.*;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.Locale;

public class LuceneReferenceResolver implements ReferenceResolver<LuceneCollectorExpression<?>> {

    private final FieldTypeLookup fieldTypeLookup;

    public LuceneReferenceResolver(FieldTypeLookup fieldTypeLookup) {
        this.fieldTypeLookup = fieldTypeLookup;
    }

    @Override
    public LuceneCollectorExpression<?> getImplementation(Reference refInfo) {
        assert refInfo.granularity() == RowGranularity.DOC: "lucene collector expressions are required to be on DOC granularity";

        ColumnIdent columnIdent = refInfo.ident().columnIdent();
        String name = columnIdent.name();
        if (RawCollectorExpression.COLUMN_NAME.equals(name)) {
            if (columnIdent.isColumn()) {
                return new RawCollectorExpression();
            } else {
                // TODO: implement an Object source expression which may support subscripts
                throw new UnsupportedFeatureException(
                    String.format(Locale.ENGLISH, "_source expression does not support subscripts %s",
                        columnIdent.fqn()));
            }
        } else if (UidCollectorExpression.COLUMN_NAME.equals(name)) {
            return new UidCollectorExpression();
        } else if (IdCollectorExpression.COLUMN_NAME.equals(name)) {
            return new IdCollectorExpression();
        } else if (DocCollectorExpression.COLUMN_NAME.equals(name)) {
            return DocCollectorExpression.create(refInfo);
        } else if (FetchIdCollectorExpression.COLUMN_NAME.equals(name)) {
            return new FetchIdCollectorExpression();
        } else if (ScoreCollectorExpression.COLUMN_NAME.equals(name)) {
            return new ScoreCollectorExpression();
        }

        String colName = columnIdent.fqn();
        MappedFieldType fieldType = fieldTypeLookup.get(colName);
        if (fieldType == null) {
            return new NullValueCollectorExpression(colName);
        }

        switch (refInfo.valueType().id()) {
            case ByteType.ID:
                return new ByteColumnReference(colName);
            case ShortType.ID:
                return new ShortColumnReference(colName);
            case IpType.ID:
                return new IpColumnReference(colName);
            case StringType.ID:
                return new BytesRefColumnReference(colName, fieldType);
            case DoubleType.ID:
                return new DoubleColumnReference(colName, fieldType);
            case BooleanType.ID:
                return new BooleanColumnReference(colName);
            case ObjectType.ID:
                return new ObjectColumnReference(colName);
            case FloatType.ID:
                return new FloatColumnReference(colName, fieldType);
            case LongType.ID:
            case TimestampType.ID:
                return new LongColumnReference(colName);
            case IntegerType.ID:
                return new IntegerColumnReference(colName);
            case GeoPointType.ID:
                return new GeoPointColumnReference(colName, fieldType);
            case GeoShapeType.ID:
                return new GeoShapeColumnReference(colName);
            case ArrayType.ID:
            case SetType.ID:
                return DocCollectorExpression.create(DocReferenceConverter.toSourceLookup(refInfo));
            default:
                throw new UnhandledServerException(String.format(Locale.ENGLISH, "unsupported type '%s'", refInfo.valueType().getName()));
        }
    }

    private static class NullValueCollectorExpression extends LuceneCollectorExpression<Void> {

        NullValueCollectorExpression(String columnName) {
            super(columnName);
        }

        @Override
        public Void value() {
            return null;
        }
    }
}
