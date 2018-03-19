/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.sort;

import com.google.common.collect.ImmutableMap;
import io.crate.data.Input;
import io.crate.execution.engine.collect.DocInputFactory;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.format.SymbolFormatter;
import io.crate.lucene.FieldTypeLookup;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.StringType;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.MultiValueMode;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SortSymbolVisitor extends SymbolVisitor<SortSymbolVisitor.SortSymbolContext, SortField> {

    private static final SortField SORT_SCORE_REVERSE = new SortField(null, SortField.Type.SCORE, true);
    private static final SortField SORT_SCORE = new SortField(null, SortField.Type.SCORE);

    public static final Map<DataType, SortField.Type> LUCENE_TYPE_MAP = ImmutableMap.<DataType, SortField.Type>builder()
        .put(DataTypes.BOOLEAN, SortField.Type.LONG)
        .put(DataTypes.BYTE, SortField.Type.LONG)
        .put(DataTypes.SHORT, SortField.Type.LONG)
        .put(DataTypes.LONG, SortField.Type.LONG)
        .put(DataTypes.INTEGER, SortField.Type.LONG)
        .put(DataTypes.FLOAT, SortField.Type.FLOAT)
        .put(DataTypes.DOUBLE, SortField.Type.DOUBLE)
        .put(DataTypes.TIMESTAMP, SortField.Type.LONG)
        .put(DataTypes.IP, SortField.Type.LONG)
        .put(DataTypes.STRING, SortField.Type.STRING)
        .build();

    static class SortSymbolContext {

        private final boolean reverseFlag;
        private final CollectorContext context;
        private final Boolean nullFirst;

        SortSymbolContext(CollectorContext collectorContext, boolean reverseFlag, Boolean nullFirst) {
            this.nullFirst = nullFirst;
            this.context = collectorContext;
            this.reverseFlag = reverseFlag;
        }
    }

    private final DocInputFactory docInputFactory;
    private final FieldTypeLookup fieldTypeLookup;

    SortSymbolVisitor(DocInputFactory docInputFactory, FieldTypeLookup fieldTypeLookup) {
        super();
        this.docInputFactory = docInputFactory;
        this.fieldTypeLookup = fieldTypeLookup;
    }

    SortField[] generateSortFields(List<Symbol> sortSymbols,
                                          CollectorContext collectorContext,
                                          boolean[] reverseFlags,
                                          Boolean[] nullsFirst) {
        SortField[] sortFields = new SortField[sortSymbols.size()];
        for (int i = 0; i < sortSymbols.size(); i++) {
            Symbol sortSymbol = sortSymbols.get(i);
            sortFields[i] = generateSortField(sortSymbol, new SortSymbolContext(collectorContext, reverseFlags[i], nullsFirst[i]));
        }
        return sortFields;
    }


    private SortField generateSortField(Symbol symbol, SortSymbolContext sortSymbolContext) {
        return process(symbol, sortSymbolContext);
    }


    /**
     * generate a SortField from a Reference symbol.
     * <p>
     * the implementation is similar to how ES 2.4 SortParseElement worked
     */
    @Override
    public SortField visitReference(final Reference symbol, final SortSymbolContext context) {
        // can't use the SortField(fieldName, type) constructor
        // because values are saved using docValues and therefore they're indexed in lucene as binary and not
        // with the reference valueType.
        // this is why we use a custom comparator source with the same logic as ES

        /*
         * TODO:
         * There is now {@link org.elasticsearch.search.sort.SortFieldAndFormat}, maybe that can be used.
         * See {@link org.elasticsearch.search.sort.ScoreSortBuilder}
         */

        ColumnIdent columnIdent = symbol.column();

        if (columnIdent.isTopLevel()) {
            if (DocSysColumns.SCORE.equals(columnIdent)) {
                return !context.reverseFlag ? SORT_SCORE_REVERSE : SORT_SCORE;
            } else if (DocSysColumns.RAW.equals(columnIdent) || DocSysColumns.ID.equals(columnIdent)) {
                return customSortField(DocSysColumns.nameForLucene(columnIdent), symbol, context, false);
            }
        }
        if (symbol.isColumnStoreDisabled()) {
            SortField.Type type = LUCENE_TYPE_MAP.get(symbol.valueType());
            return customSortField(symbol.toString(), symbol, context, type == null);
        }

        MultiValueMode sortMode = context.reverseFlag ? MultiValueMode.MAX : MultiValueMode.MIN;

        String indexName;
        FieldComparatorSource fieldComparatorSource;
        MappedFieldType fieldType = fieldTypeLookup.get(columnIdent.fqn());
        if (fieldType == null) {
            indexName = columnIdent.fqn();
            fieldComparatorSource = new NullFieldComparatorSource(LUCENE_TYPE_MAP.get(symbol.valueType()), context.reverseFlag, context.nullFirst);
            return new SortField(
                indexName,
                fieldComparatorSource,
                context.reverseFlag);
        } else {
            return context.context.fieldData()
                .getForField(fieldType)
                .sortField(SortOrder.missing(context.reverseFlag, context.nullFirst),
                    sortMode, null, context.reverseFlag);
        }

    }

    @Override
    public SortField visitFunction(final Function function, final SortSymbolContext context) {
        // our boolean functions return booleans, no BytesRefs, handle them differently
        // this is a hack, but that is how it worked before, so who cares :)
        SortField.Type type = function.valueType().equals(DataTypes.BOOLEAN) ? null : LUCENE_TYPE_MAP.get(function.valueType());
        return customSortField(function.toString(), function, context, type == null);
    }

    @Override
    protected SortField visitSymbol(Symbol symbol, SortSymbolContext context) {
        throw new UnsupportedOperationException(
            SymbolFormatter.format("Using a non-integer constant in ORDER BY is not supported", symbol));
    }

    private SortField customSortField(String name,
                                      final Symbol symbol,
                                      final SortSymbolContext context,
                                      final boolean missingNullValue) {
        InputFactory.Context<? extends LuceneCollectorExpression<?>> inputContext = docInputFactory.getCtx();
        final Input input = inputContext.add(symbol);
        final Collection<? extends LuceneCollectorExpression<?>> expressions = inputContext.expressions();

        return new SortField(name, new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String fieldName, int numHits, int sortPos, boolean reversed) {
                for (LuceneCollectorExpression collectorExpression : expressions) {
                    collectorExpression.startCollect(context.context);
                }
                DataType dataType = symbol.valueType();
                Object missingValue = missingNullValue ? null : SortSymbolVisitor.missingObject(
                    dataType,
                    SortOrder.missing(context.reverseFlag, context.nullFirst),
                    reversed);

                if (context.context.visitor().required()) {
                    return new FieldsVisitorInputFieldComparator(
                        numHits,
                        context.context.visitor(),
                        expressions,
                        input,
                        dataType,
                        missingValue
                    );

                } else {
                    return new InputFieldComparator(
                        numHits,
                        expressions,
                        input,
                        dataType,
                        missingValue
                    );
                }
            }
        }, context.reverseFlag);
    }

    /**
     * Return the missing object value according to the data type of the symbol and not the reducedType.
     * The reducedType groups non-decimal numeric types to Long and uses Long's MIN_VALUE & MAX_VALUE
     * to replace NULLs which can lead to ClassCastException when the original type was for example Integer.
     */
    @Nullable
    private static Object missingObject(DataType dataType, Object missingValue, boolean reversed) {
        boolean min = sortMissingFirst(missingValue) ^ reversed;
        switch (dataType.id()) {
            case IntegerType.ID:
                return min ? Integer.MIN_VALUE : Integer.MAX_VALUE;
            case LongType.ID:
                return min ? Long.MIN_VALUE : Long.MAX_VALUE;
            case FloatType.ID:
                return min ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
            case DoubleType.ID:
                return min ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
            case StringType.ID:
                return null;
            default:
                throw new UnsupportedOperationException("Unsupported data type: " + dataType);
        }
    }

    /** Whether missing values should be sorted first. */
    private static boolean sortMissingFirst(Object missingValue) {
        return "_first".equals(missingValue);
    }
}
