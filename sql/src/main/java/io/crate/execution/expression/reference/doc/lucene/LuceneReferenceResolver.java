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

package io.crate.execution.expression.reference.doc.lucene;

import io.crate.exceptions.UnhandledServerException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.lucene.FieldTypeLookup;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.execution.expression.reference.ReferenceResolver;
import io.crate.types.ArrayType;
import io.crate.types.BooleanType;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.GeoPointType;
import io.crate.types.GeoShapeType;
import io.crate.types.IntegerType;
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.ObjectType;
import io.crate.types.SetType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;
import org.elasticsearch.Version;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.Locale;

import static java.util.Objects.requireNonNull;

public class LuceneReferenceResolver implements ReferenceResolver<LuceneCollectorExpression<?>> {

    private final FieldTypeLookup fieldTypeLookup;
    private final IndexSettings indexSettings;

    public LuceneReferenceResolver(FieldTypeLookup fieldTypeLookup, IndexSettings indexSettings) {
        this.fieldTypeLookup = fieldTypeLookup;
        this.indexSettings = indexSettings;
    }

    @Override
    public LuceneCollectorExpression<?> getImplementation(Reference refInfo) {
        assert refInfo.granularity() == RowGranularity.DOC : "lucene collector expressions are required to be on DOC granularity";

        final DataType dataType = refInfo.valueType();
        if (dataType.id() == ObjectType.ID || dataType.id() == GeoShapeType.ID) {
            // FetchOrEval takes care of rewriting the reference to do a source lookup.
            // However, when fetching is disabled, this does not happen and we need to ensure
            // (at least) for objects that we do a source lookup.
            refInfo = DocReferences.toSourceLookup(refInfo);
        }

        ColumnIdent columnIdent = refInfo.column();
        String name = columnIdent.name();
        if (RawCollectorExpression.COLUMN_NAME.equals(name)) {
            if (columnIdent.isTopLevel()) {
                return new RawCollectorExpression();
            } else {
                // TODO: implement an Object source expression which may support subscripts
                throw new UnsupportedFeatureException(
                    String.format(Locale.ENGLISH, "_source expression does not support subscripts %s",
                        columnIdent.fqn()));
            }
        } else if (UidCollectorExpression.COLUMN_NAME.equals(name)) {
            if (indexSettings.isSingleType()) {
                // _uid and _id is the same
                return new IdCollectorExpression(
                    requireNonNull(fieldTypeLookup.get(IdCollectorExpression.COLUMN_NAME), "_id field must have a fieldType"));
            }
            return new UidCollectorExpression();
        } else if (IdCollectorExpression.COLUMN_NAME.equals(name)) {
            if (indexSettings.isSingleType()) {
                return new IdCollectorExpression(
                    requireNonNull(fieldTypeLookup.get(IdCollectorExpression.COLUMN_NAME), "_id field must have a fieldType"));
            }
            return new IdFromUidCollectorExpression();
        } else if (DocCollectorExpression.COLUMN_NAME.equals(name)) {
            return DocCollectorExpression.create(refInfo);
        } else if (FetchIdCollectorExpression.COLUMN_NAME.equals(name)) {
            return new FetchIdCollectorExpression();
        } else if (ScoreCollectorExpression.COLUMN_NAME.equals(name)) {
            return new ScoreCollectorExpression();
        } else if (DocSysColumns.VERSION.name().equals(name)) {
            return new VersionCollectorExpression();
        }

        String fqn = columnIdent.fqn();
        MappedFieldType fieldType = fieldTypeLookup.get(fqn);
        if (fieldType == null) {
            return new NullValueCollectorExpression(fqn);
        }
        if (fieldType.hasDocValues() == false) {
            Reference ref = DocReferences.toSourceLookup(refInfo);
            return DocCollectorExpression.create(ref);
        }

        switch (dataType.id()) {
            case ByteType.ID:
                return new ByteColumnReference(fqn);
            case ShortType.ID:
                return new ShortColumnReference(fqn);
            case IpType.ID:
                Version indexVersionCreated = indexSettings.getIndexVersionCreated();
                if (indexVersionCreated.before(Version.V_5_0_0)) {
                    return new LegacyIPColumnReference(fqn);
                }
                return new IpColumnReference(fqn);
            case StringType.ID:
                return new BytesRefColumnReference(fqn, fieldType);
            case DoubleType.ID:
                return new DoubleColumnReference(fqn, fieldType);
            case BooleanType.ID:
                return new BooleanColumnReference(fqn);
            case FloatType.ID:
                return new FloatColumnReference(fqn, fieldType);
            case LongType.ID:
            case TimestampType.ID:
                return new LongColumnReference(fqn);
            case IntegerType.ID:
                return new IntegerColumnReference(fqn);
            case GeoPointType.ID:
                return new GeoPointColumnReference(fqn, fieldType);
            case ArrayType.ID:
            case SetType.ID:
                return DocCollectorExpression.create(DocReferences.toSourceLookup(refInfo));
            default:
                throw new UnhandledServerException(String.format(Locale.ENGLISH, "unsupported type '%s'", dataType.getName()));
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
