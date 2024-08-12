/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import java.util.List;

import io.crate.exceptions.UnhandledServerException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.VoidReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocReferences;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.BitStringType;
import io.crate.types.BooleanType;
import io.crate.types.ByteType;
import io.crate.types.CharacterType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.FloatVectorType;
import io.crate.types.GeoPointType;
import io.crate.types.IntegerType;
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;

public class LuceneReferenceResolver implements ReferenceResolver<LuceneCollectorExpression<?>> {

    private final List<Reference> partitionColumns;
    private final String indexName;

    public LuceneReferenceResolver(final String indexName,
                                   final List<Reference> partitionColumns) {
        this.indexName = indexName;
        this.partitionColumns = partitionColumns;
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public LuceneCollectorExpression<?> getImplementation(final Reference ref) {
        final ColumnIdent column = ref.column();
        switch (column.name()) {
            case DocSysColumns.Names.RAW:
                if (column.isRoot()) {
                    return new RawCollectorExpression();
                }
                throw new UnsupportedFeatureException("_raw expression does not support subscripts: " + column);

            case DocSysColumns.Names.UID:
            case DocSysColumns.Names.ID:
                return new IdCollectorExpression();

            case DocSysColumns.Names.FETCHID:
                return new FetchIdCollectorExpression();

            case DocSysColumns.Names.DOCID:
                return new DocIdCollectorExpression();

            case DocSysColumns.Names.SCORE:
                return new ScoreCollectorExpression();

            case DocSysColumns.Names.VERSION:
                return new VersionCollectorExpression();

            case DocSysColumns.Names.SEQ_NO:
                return new SeqNoCollectorExpression();

            case DocSysColumns.Names.PRIMARY_TERM:
                return new PrimaryTermCollectorExpression();

            case DocSysColumns.Names.DOC: {
                return DocCollectorExpression.create(ref);
            }

            default: {
                int partitionPos = Reference.indexOf(partitionColumns, column);
                if (partitionPos >= 0) {
                    return new LiteralValueExpression(
                        ref.valueType().implicitCast(PartitionName.fromIndexOrTemplate(indexName).values().get(partitionPos))
                    );
                }
                return typeSpecializedExpression(ref);
            }
        }
    }

    private static LuceneCollectorExpression<?> typeSpecializedExpression(final Reference ref) {
        final String fqn = ref.storageIdent();
        // non-ignored dynamic references should have been resolved to void references by this point
        if (ref instanceof VoidReference) {
            return new LiteralValueExpression(null);
        }
        assert ref instanceof DynamicReference == false || ref.columnPolicy() == ColumnPolicy.IGNORED;
        if (ref.hasDocValues() == false) {
            return DocCollectorExpression.create(DocReferences.toDocLookup(ref));
        }
        return switch (ref.valueType().id()) {
            case BitStringType.ID -> new BitStringColumnReference(fqn, ((BitStringType) ref.valueType()).length());
            case ByteType.ID -> new ByteColumnReference(fqn);
            case ShortType.ID -> new ShortColumnReference(fqn);
            case IpType.ID -> new IpColumnReference(fqn);
            case StringType.ID, CharacterType.ID -> new StringColumnReference(fqn);
            case DoubleType.ID -> new DoubleColumnReference(fqn);
            case BooleanType.ID -> new BooleanColumnReference(fqn);
            case FloatType.ID -> new FloatColumnReference(fqn);
            case LongType.ID, TimestampType.ID_WITH_TZ, TimestampType.ID_WITHOUT_TZ -> new LongColumnReference(fqn);
            case IntegerType.ID -> new IntegerColumnReference(fqn);
            case GeoPointType.ID -> new GeoPointColumnReference(fqn);
            case ArrayType.ID -> DocCollectorExpression.create(DocReferences.toDocLookup(ref));
            case FloatVectorType.ID -> new FloatVectorColumnReference(fqn);
            default -> throw new UnhandledServerException("Unsupported type: " + ref.valueType().getName());
        };
    }

    static class LiteralValueExpression extends LuceneCollectorExpression<Object> {

        private final Object value;

        public LiteralValueExpression(Object value) {
            this.value = value;
        }

        @Override
        public Object value() {
            return value;
        }
    }

}
