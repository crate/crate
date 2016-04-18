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
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.RowGranularity;
import io.crate.operation.reference.ReferenceResolver;
import io.crate.types.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.mapper.MapperService;

import java.util.Locale;

public class LuceneReferenceResolver implements ReferenceResolver<LuceneCollectorExpression<?>> {

    private final @Nullable MapperService mapperService;

    private final static NullValueCollectorExpression NULL_COLLECTOR_EXPRESSION = new NullValueCollectorExpression();

    public LuceneReferenceResolver(@Nullable MapperService mapperService) {
        this.mapperService = mapperService;
    }

    @Override
    public LuceneCollectorExpression<?> getImplementation(ReferenceInfo refInfo) {
        assert refInfo.granularity() == RowGranularity.DOC;

        if (RawCollectorExpression.COLUMN_NAME.equals(refInfo.ident().columnIdent().name())){
            if (refInfo.ident().columnIdent().isColumn()){
                return new RawCollectorExpression();
            } else {
                // TODO: implement an Object source expression which may support subscripts
                throw new UnsupportedFeatureException(
                        String.format(Locale.ENGLISH, "_source expression does not support subscripts %s",
                        refInfo.ident().columnIdent().fqn()));
            }
        } else if (IdCollectorExpression.COLUMN_NAME.equals(refInfo.ident().columnIdent().name())) {
            return new IdCollectorExpression();
        } else if (DocCollectorExpression.COLUMN_NAME.equals(refInfo.ident().columnIdent().name())) {
            return DocCollectorExpression.create(refInfo);
        } else if (DocIdCollectorExpression.COLUMN_NAME.equals(refInfo.ident().columnIdent().name())) {
            return new DocIdCollectorExpression();
        } else if (ScoreCollectorExpression.COLUMN_NAME.equals(refInfo.ident().columnIdent().name())) {
            return new ScoreCollectorExpression();
        }

        String colName = refInfo.ident().columnIdent().fqn();
        if (this.mapperService != null && mapperService.smartNameFieldType(colName) == null) {
            return NULL_COLLECTOR_EXPRESSION;
        }

        switch (refInfo.type().id()) {
            case ByteType.ID:
                return new ByteColumnReference(colName);
            case ShortType.ID:
                return new ShortColumnReference(colName);
            case IpType.ID:
                return new IpColumnReference(colName);
            case StringType.ID:
                return new BytesRefColumnReference(colName);
            case DoubleType.ID:
                return new DoubleColumnReference(colName);
            case BooleanType.ID:
                return new BooleanColumnReference(colName);
            case ObjectType.ID:
                return new ObjectColumnReference(colName);
            case FloatType.ID:
                return new FloatColumnReference(colName);
            case LongType.ID:
            case TimestampType.ID:
                return new LongColumnReference(colName);
            case IntegerType.ID:
                return new IntegerColumnReference(colName);
            case GeoPointType.ID:
                return new GeoPointColumnReference(colName);
            case GeoShapeType.ID:
                return new GeoShapeColumnReference(colName);
            default:
                throw new UnhandledServerException(String.format(Locale.ENGLISH, "unsupported type '%s'", refInfo.type().getName()));
        }
    }

    private static class NullValueCollectorExpression extends LuceneCollectorExpression<Void> {

        @Override
        public Void value() {
            return null;
        }
    }
}
