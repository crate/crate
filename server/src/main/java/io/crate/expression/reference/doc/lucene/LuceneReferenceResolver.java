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
import java.util.function.Predicate;

import org.elasticsearch.Version;

import io.crate.exceptions.UnhandledServerException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.VoidReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.SysColumns;
import io.crate.types.ArrayType;
import io.crate.types.BitStringType;
import io.crate.types.BooleanType;
import io.crate.types.ByteType;
import io.crate.types.CharacterType;
import io.crate.types.DataType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.FloatVectorType;
import io.crate.types.GeoPointType;
import io.crate.types.IntegerType;
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.NumericStorage;
import io.crate.types.NumericType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;

public class LuceneReferenceResolver implements ReferenceResolver<LuceneCollectorExpression<?>> {

    private final List<Reference> partitionColumns;
    private final List<String> partitionValues;
    private final Predicate<Reference> isParentRefIgnored;
    private final Predicate<ColumnIdent> isSingletonPrimaryKey;
    private final Version shardVersion;

    public LuceneReferenceResolver(final List<String> partitionValues,
                                   final List<Reference> partitionColumns,
                                   final List<ColumnIdent> primaryKey,
                                   Version shardVersion,
                                   Predicate<Reference> isParentRefIgnored) {
        this.partitionValues = partitionValues;
        this.partitionColumns = partitionColumns;
        this.isParentRefIgnored = isParentRefIgnored;
        this.shardVersion = shardVersion;
        this.isSingletonPrimaryKey = primaryKey.size() == 1 ? c -> primaryKey.getFirst().equals(c) : _ -> false;
    }

    public List<String> getPartitionValues() {
        return partitionValues;
    }

    @Override
    public LuceneCollectorExpression<?> getImplementation(final Reference ref) {
        final ColumnIdent column = ref.column();
        if (ref.valueType() instanceof StringType && isSingletonPrimaryKey.test(column)) {
            return IdCollectorExpression.forVersion(shardVersion);
        }
        switch (column.name()) {
            case SysColumns.Names.RAW:
                if (column.isRoot()) {
                    return new RawCollectorExpression();
                }
                throw new UnsupportedFeatureException("_raw expression does not support subscripts: " + column);

            case SysColumns.Names.UID:
            case SysColumns.Names.ID:
                return IdCollectorExpression.forVersion(shardVersion);

            case SysColumns.Names.FETCHID:
                return new FetchIdCollectorExpression();

            case SysColumns.Names.DOCID:
                return new DocIdCollectorExpression();

            case SysColumns.Names.SCORE:
                return new ScoreCollectorExpression();

            case SysColumns.Names.VERSION:
                return new VersionCollectorExpression();

            case SysColumns.Names.SEQ_NO:
                return new SeqNoCollectorExpression();

            case SysColumns.Names.PRIMARY_TERM:
                return new PrimaryTermCollectorExpression();

            case SysColumns.Names.DOC: {
                return DocCollectorExpression.create(ref, isParentRefIgnored);
            }

            default: {
                int partitionPos = Reference.indexOf(partitionColumns, column);
                if (partitionPos >= 0) {
                    return new LiteralValueExpression(
                        ref.valueType().implicitCast(partitionValues.get(partitionPos))
                    );
                }
                return typeSpecializedExpression(ref, isParentRefIgnored);
            }
        }
    }

    public static LuceneCollectorExpression<?> typeSpecializedExpression(final Reference ref,
                                                                         Predicate<Reference> isParentRefIgnored) {
        final String fqn = ref.storageIdent();
        // non-ignored dynamic references should have been resolved to void references by this point
        if (ref instanceof VoidReference) {
            return new LiteralValueExpression(null);
        }
        if (ref.hasDocValues() == false) {
            return DocCollectorExpression.create(DocReferences.toDocLookup(ref), isParentRefIgnored);
        }
        DataType<?> valueType = ref.valueType();
        return switch (valueType.id()) {
            case BitStringType.ID -> new BitStringColumnReference(fqn, ((BitStringType) valueType).length());
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
            case ArrayType.ID -> DocCollectorExpression.create(DocReferences.toDocLookup(ref), isParentRefIgnored);
            case FloatVectorType.ID -> new FloatVectorColumnReference(fqn);
            case NumericType.ID -> NumericStorage.getCollectorExpression(fqn, (NumericType) valueType);
            default -> throw new UnhandledServerException("Unsupported type: " + valueType.getName());
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
