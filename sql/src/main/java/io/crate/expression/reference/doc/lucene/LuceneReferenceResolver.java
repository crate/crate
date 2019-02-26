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

import com.google.common.collect.ImmutableSet;
import io.crate.exceptions.UnhandledServerException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.lucene.FieldTypeLookup;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.ArrayType;
import io.crate.types.BooleanType;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.GeoPointType;
import io.crate.types.IntegerType;
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.SetType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.Set;

import static io.crate.metadata.DocReferences.toSourceLookup;
import static io.crate.types.CollectionType.unnest;

public class LuceneReferenceResolver implements ReferenceResolver<LuceneCollectorExpression<?>> {

    private static final Set<DataType<?>> NO_FIELD_TYPES = ImmutableSet.of(DataTypes.OBJECT, DataTypes.GEO_SHAPE);
    private final FieldTypeLookup fieldTypeLookup;
    private final IndexSettings indexSettings;

    public LuceneReferenceResolver(FieldTypeLookup fieldTypeLookup, IndexSettings indexSettings) {
        this.fieldTypeLookup = fieldTypeLookup;
        this.indexSettings = indexSettings;
    }

    @Override
    public LuceneCollectorExpression<?> getImplementation(Reference ref) {
        assert ref.granularity() == RowGranularity.DOC : "lucene collector expressions are required to be on DOC granularity";

        ColumnIdent column = ref.column();
        switch (column.name()) {
            case DocSysColumns.Names.RAW:
                if (column.isTopLevel()) {
                    return new RawCollectorExpression();
                }
                throw new UnsupportedFeatureException("_raw expression does not support subscripts: " + column);

            case DocSysColumns.Names.UID:
            case DocSysColumns.Names.ID:
                return new IdCollectorExpression();

            case DocSysColumns.Names.DOC:
                return DocCollectorExpression.create(ref);

            case DocSysColumns.Names.FETCHID:
                return new FetchIdCollectorExpression();

            case DocSysColumns.Names.SCORE:
                return new ScoreCollectorExpression();

            case DocSysColumns.Names.VERSION:
                return new VersionCollectorExpression();

            default:
                return typeSpecializedExpression(fieldTypeLookup, ref);
        }
    }

    private static LuceneCollectorExpression<?> typeSpecializedExpression(FieldTypeLookup fieldTypeLookup, Reference ref) {
        String fqn = ref.column().fqn();
        MappedFieldType fieldType = fieldTypeLookup.get(fqn);
        if (fieldType == null) {
            return NO_FIELD_TYPES.contains(unnest(ref.valueType()))
                ? DocCollectorExpression.create(toSourceLookup(ref))
                : new NullValueCollectorExpression();
        }
        if (!fieldType.hasDocValues()) {
            return DocCollectorExpression.create(toSourceLookup(ref));
        }
        switch (ref.valueType().id()) {
            case ByteType.ID:
                return new ByteColumnReference(fqn);
            case ShortType.ID:
                return new ShortColumnReference(fqn);
            case IpType.ID:
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
                return DocCollectorExpression.create(toSourceLookup(ref));
            default:
                throw new UnhandledServerException("Unsupported type: " + ref.valueType().getName());
        }
    }

    private static class NullValueCollectorExpression extends LuceneCollectorExpression<Void> {

        @Override
        public Void value() {
            return null;
        }
    }
}
