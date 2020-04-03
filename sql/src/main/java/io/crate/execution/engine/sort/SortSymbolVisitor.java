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
import io.crate.lucene.FieldTypeLookup;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.SortField;
import org.elasticsearch.index.fielddata.NullValueOrder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.MultiValueMode;

import java.util.Comparator;
import java.util.List;

public class SortSymbolVisitor extends SymbolVisitor<SortSymbolVisitor.SortSymbolContext, SortField> {

    private static final SortField SORT_SCORE_REVERSE = new SortField(null, SortField.Type.SCORE, true);
    private static final SortField SORT_SCORE = new SortField(null, SortField.Type.SCORE);

    static class SortSymbolContext {

        private final boolean reverseFlag;
        private final CollectorContext context;
        private final TransactionContext txnCtx;
        private final boolean nullFirst;

        SortSymbolContext(TransactionContext txnCtx,
                          CollectorContext collectorContext,
                          boolean reverseFlag,
                          boolean nullFirst) {
            this.txnCtx = txnCtx;
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
                                   TransactionContext txnCtx,
                                   CollectorContext collectorContext,
                                   boolean[] reverseFlags,
                                   boolean[] nullsFirst) {
        SortField[] sortFields = new SortField[sortSymbols.size()];
        for (int i = 0; i < sortSymbols.size(); i++) {
            Symbol sortSymbol = sortSymbols.get(i);
            sortFields[i] = sortSymbol.accept(this, new SortSymbolContext(txnCtx, collectorContext, reverseFlags[i], nullsFirst[i]));
        }
        return sortFields;
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

        ColumnIdent columnIdent = symbol.column();
        if (DocSysColumns.SCORE.equals(columnIdent)) {
            return !context.reverseFlag ? SORT_SCORE_REVERSE : SORT_SCORE;
        }
        if (DocSysColumns.RAW.equals(columnIdent) || DocSysColumns.ID.equals(columnIdent)) {
            return customSortField(DocSysColumns.nameForLucene(columnIdent), symbol, context);
        }
        if (symbol.isColumnStoreDisabled()) {
            return customSortField(symbol.toString(), symbol, context);
        }

        MultiValueMode sortMode = context.reverseFlag ? MultiValueMode.MAX : MultiValueMode.MIN;
        MappedFieldType fieldType = fieldTypeLookup.get(columnIdent.fqn());
        if (fieldType == null) {
            FieldComparatorSource fieldComparatorSource = new NullFieldComparatorSource(NullSentinelValues.nullSentinelForScoreDoc(
                symbol.valueType(),
                context.reverseFlag,
                context.nullFirst
            ));
            return new SortField(
                columnIdent.fqn(),
                fieldComparatorSource,
                context.reverseFlag);
        } else if (symbol.valueType().equals(DataTypes.IP)) {
            return customSortField(symbol.toString(), symbol, context);
        } else {
            return context.context.getFieldData(fieldType)
                .sortField(NullValueOrder.fromFlag(context.nullFirst), sortMode, context.reverseFlag);
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
            public FieldComparator<?> newComparator(String fieldName, int numHits, int sortPos, boolean reversed) {
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
