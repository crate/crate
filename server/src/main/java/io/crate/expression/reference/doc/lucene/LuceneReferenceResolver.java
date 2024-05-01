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

import static io.crate.metadata.DocReferences.toSourceLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Scorable;

import io.crate.common.collections.Maps;
import io.crate.exceptions.UnhandledServerException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.VoidReference;
import io.crate.metadata.ColumnIdent;
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
                var result = DocCollectorExpression.create(ref);
                return maybeInjectPartitionValue(
                    result,
                    indexName,
                    partitionColumns,
                    column.isRoot() ? column : column.shiftRight()  // Remove `_doc` prefix so that it can match the column against partitionColumns
                );
            }

            default: {
                int partitionPos = Reference.indexOf(partitionColumns, column);
                if (partitionPos >= 0) {
                    return new LiteralValueExpression(
                        ref.valueType().implicitCast(PartitionName.fromIndexOrTemplate(indexName).values().get(partitionPos))
                    );
                }
                return maybeInjectPartitionValue(
                    typeSpecializedExpression(ref),
                    indexName,
                    partitionColumns,
                    column
                );
            }
        }
    }

    private static LuceneCollectorExpression<?> maybeInjectPartitionValue(LuceneCollectorExpression<?> result,
                                                                          String indexName,
                                                                          List<Reference> partitionColumns,
                                                                          ColumnIdent column) {
        for (int i = 0; i < partitionColumns.size(); i++) {
            final Reference partitionColumn = partitionColumns.get(i);
            final var partitionColumnIdent = partitionColumn.column();
            if (partitionColumnIdent.isChildOf(column)) {
                return new PartitionValueInjectingExpression(
                    PartitionName.fromIndexOrTemplate(indexName),
                    i,
                    partitionColumnIdent.shiftRight(),
                    result
                );
            }
        }
        return result;
    }

    private static LuceneCollectorExpression<?> typeSpecializedExpression(final Reference ref) {
        final String fqn = ref.storageIdent();
        // non-ignored dynamic references should have been resolved to void references by this point
        if (ref instanceof VoidReference) {
            return new LiteralValueExpression(null);
        }
        assert ref instanceof DynamicReference == false || ref.columnPolicy() == ColumnPolicy.IGNORED;
        if (ref.hasDocValues() == false) {
            return DocCollectorExpression.create(toSourceLookup(ref));
        }
        return switch (ref.valueType().id()) {
            case BitStringType.ID -> new BitStringColumnReference(fqn, ((BitStringType) ref.valueType()).length());
            case ByteType.ID -> new ByteColumnReference(fqn);
            case ShortType.ID -> new ShortColumnReference(fqn);
            case IpType.ID -> new IpColumnReference(fqn);
            case StringType.ID, CharacterType.ID -> new BytesRefColumnReference(fqn);
            case DoubleType.ID -> new DoubleColumnReference(fqn);
            case BooleanType.ID -> new BooleanColumnReference(fqn);
            case FloatType.ID -> new FloatColumnReference(fqn);
            case LongType.ID, TimestampType.ID_WITH_TZ, TimestampType.ID_WITHOUT_TZ -> new LongColumnReference(fqn);
            case IntegerType.ID -> new IntegerColumnReference(fqn);
            case GeoPointType.ID -> new GeoPointColumnReference(fqn);
            case ArrayType.ID -> DocCollectorExpression.create(toSourceLookup(ref));
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


    static class PartitionValueInjectingExpression extends LuceneCollectorExpression<Object> {

        private final LuceneCollectorExpression<?> inner;
        private final ColumnIdent partitionPath;
        private final int partitionPos;
        private final PartitionName partitionName;

        public PartitionValueInjectingExpression(PartitionName partitionName,
                                                 int partitionPos,
                                                 ColumnIdent partitionPath,
                                                 LuceneCollectorExpression<?> inner) {
            this.inner = inner;
            this.partitionName = partitionName;
            this.partitionPos = partitionPos;
            this.partitionPath = partitionPath;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object value() {
            final var object = (Map<String, Object>) inner.value();
            final var partitionValue = partitionName.values().get(partitionPos);
            Maps.mergeInto(
                object,
                partitionPath.name(),
                partitionPath.path(),
                partitionValue
            );
            return object;
        }

        @Override
        public void startCollect(final CollectorContext context) {
            inner.startCollect(context);
        }

        @Override
        public void setNextDocId(final int doc) {
            inner.setNextDocId(doc);
        }

        @Override
        public void setNextReader(ReaderContext context) throws IOException {
            inner.setNextReader(context);
        }

        @Override
        public void setScorer(final Scorable scorer) {
            inner.setScorer(scorer);
        }
    }
}
