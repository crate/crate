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

import static io.crate.metadata.DocReferences.toSourceLookup;
import static io.crate.types.ArrayType.unnest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.crate.types.TimeTZType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.elasticsearch.index.mapper.MappedFieldType;

import io.crate.common.collections.Maps;
import io.crate.exceptions.UnhandledServerException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.SymbolType;
import io.crate.lucene.FieldTypeLookup;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.BooleanType;
import io.crate.types.ByteType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.GeoPointType;
import io.crate.types.GeoShapeType;
import io.crate.types.IntegerType;
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.ObjectType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;

public class LuceneReferenceResolver implements ReferenceResolver<LuceneCollectorExpression<?>> {

    private static final Set<Integer> NO_FIELD_TYPES_IDS = Set.of(ObjectType.ID, GeoShapeType.ID);
    private final FieldTypeLookup fieldTypeLookup;
    private final List<Reference> partitionColumns;
    private final String indexName;

    public LuceneReferenceResolver(final String indexName,
                                   final FieldTypeLookup fieldTypeLookup,
                                   final List<Reference> partitionColumns) {
        this.indexName = indexName;
        this.fieldTypeLookup = fieldTypeLookup;
        this.partitionColumns = partitionColumns;
    }

    @Override
    public LuceneCollectorExpression<?> getImplementation(final Reference ref) {
        final ColumnIdent column = ref.column();
        switch (column.name()) {
            case DocSysColumns.Names.RAW:
                if (column.isTopLevel()) {
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
                    column.isTopLevel() ? column : column.shiftRight()  // Remove `_doc` prefix so that it can match the column against partitionColumns
                );
            }

            default: {
                return maybeInjectPartitionValue(
                    typeSpecializedExpression(fieldTypeLookup, ref),
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

    private static LuceneCollectorExpression<?> typeSpecializedExpression(final FieldTypeLookup fieldTypeLookup, final Reference ref) {
        final String fqn = ref.column().fqn();
        final MappedFieldType fieldType = fieldTypeLookup.get(fqn);
        if (fieldType == null) {
            return NO_FIELD_TYPES_IDS.contains(unnest(ref.valueType()).id()) || isIgnoredDynamicReference(ref)
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
                return new BytesRefColumnReference(fqn);
            case DoubleType.ID:
                return new DoubleColumnReference(fqn);
            case BooleanType.ID:
                return new BooleanColumnReference(fqn);
            case FloatType.ID:
                return new FloatColumnReference(fqn);
            case LongType.ID:
            case TimeTZType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
                return new LongColumnReference(fqn);
            case IntegerType.ID:
                return new IntegerColumnReference(fqn);
            case GeoPointType.ID:
                return new GeoPointColumnReference(fqn);
            case ArrayType.ID:
                return DocCollectorExpression.create(toSourceLookup(ref));
            default:
                throw new UnhandledServerException("Unsupported type: " + ref.valueType().getName());
        }
    }

    private static boolean isIgnoredDynamicReference(final Reference ref) {
        return ref.symbolType() == SymbolType.DYNAMIC_REFERENCE && ref.columnPolicy() == ColumnPolicy.IGNORED;
    }

    private static class NullValueCollectorExpression extends LuceneCollectorExpression<Void> {

        @Override
        public Void value() {
            return null;
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

        public void startCollect(final CollectorContext context) {
            inner.startCollect(context);
        }

        public void setNextDocId(final int doc) {
            inner.setNextDocId(doc);
        }

        public void setNextReader(final LeafReaderContext context) throws IOException {
            inner.setNextReader(context);
        }

        public void setScorer(final Scorable scorer) {
            inner.setScorer(scorer);
        }
    }
}
