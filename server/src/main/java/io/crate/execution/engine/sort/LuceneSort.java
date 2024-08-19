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

package io.crate.execution.engine.sort;

import java.util.Comparator;
import java.util.List;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.analyze.OrderBy;
import io.crate.data.Input;
import io.crate.execution.engine.collect.DocInputFactory;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.NullSentinelValues;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.BitStringType;
import io.crate.types.BooleanType;
import io.crate.types.ByteType;
import io.crate.types.CharacterType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.FloatVectorType;
import io.crate.types.GeoPointType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.NumericStorage;
import io.crate.types.NumericType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;

public class LuceneSort extends SymbolVisitor<LuceneSort.SortSymbolContext, SortField> {

    private static final SortField SORT_SCORE_REVERSE = new SortField(null, SortField.Type.SCORE, true);
    private static final SortField SORT_SCORE = new SortField(null, SortField.Type.SCORE);


    @Nullable
    public static Sort generate(TransactionContext txnCtx,
                                CollectorContext context,
                                OrderBy orderBy,
                                DocInputFactory docInputFactory) {
        if (orderBy.orderBySymbols().isEmpty()) {
            return null;
        }
        LuceneSort luceneSort = new LuceneSort(docInputFactory);
        SortField[] sortFields = luceneSort.generateSortFields(
            orderBy.orderBySymbols(),
            txnCtx,
            context,
            orderBy.reverseFlags(),
            orderBy.nullsFirst()
        );
        return new Sort(sortFields);
    }

    static record SortSymbolContext(TransactionContext txnCtx,
                                    CollectorContext context,
                                    boolean reverseFlag,
                                    boolean nullFirst){
    }

    private final DocInputFactory docInputFactory;

    private LuceneSort(DocInputFactory docInputFactory) {
        this.docInputFactory = docInputFactory;
    }

    private SortField[] generateSortFields(List<Symbol> sortSymbols,
                                           TransactionContext txnCtx,
                                           CollectorContext collectorContext,
                                           boolean[] reverseFlags,
                                           boolean[] nullsFirst) {
        SortField[] sortFields = new SortField[sortSymbols.size()];
        for (int i = 0; i < sortSymbols.size(); i++) {
            Symbol sortSymbol = sortSymbols.get(i);
            SortSymbolContext sortSymbolContext = new SortSymbolContext(txnCtx, collectorContext, reverseFlags[i], nullsFirst[i]);
            sortFields[i] = sortSymbol.accept(this, sortSymbolContext);
        }
        return sortFields;
    }

    /**
     * generate a SortField from a Reference symbol.
     * <p>
     * the implementation is similar to how ES 2.4 SortParseElement worked
     */
    @Override
    public SortField visitReference(Reference ref, final SortSymbolContext context) {
        // can't use the SortField(fieldName, type) constructor
        // because values are saved using docValues and therefore they're indexed in lucene as binary and not
        // with the reference valueType.
        // this is why we use a custom comparator source with the same logic as ES


        // Always prefer doc-values. Should be faster for sorting and otherwise we'd
        // default to the `NullFieldComparatorSource` - leading to `null` values.
        if (ref.column().isChildOf(DocSysColumns.DOC)) {
            ref = (Reference) DocReferences.inverseSourceLookup(ref);
        }

        ColumnIdent columnIdent = ref.column();
        if (DocSysColumns.SCORE.equals(columnIdent)) {
            return !context.reverseFlag ? SORT_SCORE_REVERSE : SORT_SCORE;
        }
        if (DocSysColumns.RAW.equals(columnIdent) || DocSysColumns.ID.COLUMN.equals(columnIdent)) {
            return customSortField(DocSysColumns.nameForLucene(columnIdent), ref, context);
        }
        if (!ref.hasDocValues()) {
            return customSortField(ref.toString(), ref, context);
        }

        if (ref.valueType() instanceof NumericType numericType) {
            Integer precision = numericType.numericPrecision();
            if (precision == null || precision > NumericStorage.COMPACT_PRECISION) {
                return customSortField(ref.toString(), ref, context);
            }
            var sortField = new SortedNumericSortField(
                ref.storageIdent(),
                SortField.Type.LONG,
                context.reverseFlag,
                context.reverseFlag ? SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN
            );
            boolean min = NullValueOrder.fromFlag(context.nullFirst) == NullValueOrder.FIRST ^ context.reverseFlag;
            sortField.setMissingValue(
                min ? NumericStorage.COMPACT_MIN_VALUE - 1 : NumericStorage.COMPACT_MAX_VALUE + 1);
            return sortField;
        }

        if (ref.valueType().equals(DataTypes.IP)
                || ref.valueType().id() == BitStringType.ID
                || ref.valueType().id() == FloatVectorType.ID) {
            return customSortField(ref.toString(), ref, context);
        } else {
            NullValueOrder nullValueOrder = NullValueOrder.fromFlag(context.nullFirst);
            return mappedSortField(ref, context.reverseFlag, nullValueOrder);
        }
    }

    @VisibleForTesting
    static SortField mappedSortField(Reference symbol,
                                     boolean reverse,
                                     NullValueOrder nullValueOrder) {
        String fieldName = symbol.storageIdent();
        DataType<?> valueType = symbol.valueType();
        switch (valueType.id()) {
            case StringType.ID, CharacterType.ID -> {
                SortField sortField = new SortedSetSortField(
                    fieldName,
                    reverse,
                    reverse ? SortedSetSelector.Type.MAX : SortedSetSelector.Type.MIN
                );
                sortField.setMissingValue(
                    nullValueOrder == NullValueOrder.LAST ^ reverse
                        ? SortedSetSortField.STRING_LAST
                        : SortedSetSortField.STRING_FIRST
                );
                return sortField;
            }
            case BooleanType.ID, ByteType.ID, ShortType.ID, IntegerType.ID, LongType.ID, TimestampType.ID_WITHOUT_TZ, TimestampType.ID_WITH_TZ -> {
                var selectorType = reverse ? SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN;
                var sortField = new SortedNumericSortField(fieldName, SortField.Type.LONG, reverse, selectorType);

                /*  https://github.com/apache/lucene/commit/cc58c5194129e213877f11e002f7670d4f4bdf63
                    > Sort optimization has a number of requirements, one of which is that SortField.Type matches the Point type
                      with which the field was indexed (e.g. sort on IntPoint field should use SortField.Type.INT).

                    The first requirement is not met (e.g. sort on IntPoint uses SortField.Type.LONG) so need to set it to false.  */
                sortField.setOptimizeSortWithPoints(false);

                sortField.setMissingValue(
                    NullSentinelValues.nullSentinel(DataTypes.LONG, nullValueOrder, reverse));
                return sortField;
            }
            case FloatType.ID -> {
                var selectorType = reverse ? SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN;
                var sortField = new SortedNumericSortField(fieldName, SortField.Type.FLOAT, reverse, selectorType);
                sortField.setMissingValue(
                    NullSentinelValues.nullSentinel(DataTypes.FLOAT, nullValueOrder, reverse));
                return sortField;
            }
            case DoubleType.ID -> {
                var selectorType = reverse ? SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN;
                var sortField = new SortedNumericSortField(fieldName, SortField.Type.DOUBLE, reverse, selectorType);
                sortField.setMissingValue(
                    NullSentinelValues.nullSentinel(DataTypes.DOUBLE, nullValueOrder, reverse));
                return sortField;
            }
            case GeoPointType.ID -> throw new IllegalArgumentException(
                "can't sort on geo_point field without using specific sorting feature, like geo_distance");
            default -> throw new UnsupportedOperationException("Cannot order on " + symbol + "::" + valueType);
        }
    }

    @Override
    public SortField visitFunction(final Function function, final SortSymbolContext context) {
        return customSortField(function.toString(), function, context);
    }

    @Override
    public SortField visitAlias(AliasSymbol aliasSymbol, SortSymbolContext context) {
        return aliasSymbol.symbol().accept(this, context);
    }

    @Override
    protected SortField visitSymbol(Symbol symbol, SortSymbolContext context) {
        throw new UnsupportedOperationException(
                Symbols.format("Using a non-integer constant in ORDER BY is not supported", symbol));
    }

    private SortField customSortField(String name,
                                      final Symbol symbol,
                                      final SortSymbolContext context) {
        InputFactory.Context<? extends LuceneCollectorExpression<?>> inputContext = docInputFactory.getCtx(context.txnCtx);
        final Input<?> input = inputContext.add(symbol);
        final List<? extends LuceneCollectorExpression<?>> expressions = inputContext.expressions();
        final CollectorContext collectorContext = context.context;
        final boolean nullFirst = context.nullFirst;

        return new SortField(name, new FieldComparatorSource() {

            @Override
            public FieldComparator<?> newComparator(String fieldname,
                                                    int numHits,
                                                    Pruning pruning,
                                                    boolean reversed) {
                for (int i = 0; i < expressions.size(); i++) {
                    expressions.get(i).startCollect(collectorContext);
                }
                @SuppressWarnings("unchecked")
                DataType<Object> dataType = (DataType<Object>) symbol.valueType();
                Object nullSentinel = NullSentinelValues.nullSentinel(
                    dataType,
                    NullValueOrder.fromFlag(nullFirst),
                    reversed);
                return new InputFieldComparator(
                    numHits,
                    expressions,
                    input,
                    // for non `null` sentinel values, the nullSentinel already implies reverse+nullsFirst logic
                    // for `null` sentinels we need to have a comparator that can deal with that
                    nullSentinel == null
                        ? nullFirst ^ reversed ? Comparator.nullsFirst(dataType) : Comparator.nullsLast(dataType)
                        : dataType,
                    nullSentinel
                );
            }
        }, context.reverseFlag);
    }
}
