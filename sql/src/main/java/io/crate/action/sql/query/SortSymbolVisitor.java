/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.action.sql.query;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import io.crate.executor.transport.task.elasticsearch.SortOrder;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.planner.symbol.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortParseElement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SortSymbolVisitor extends SymbolVisitor<SortSymbolVisitor.SortSymbolContext, SortField> {

    public static final Map<DataType, SortField.Type> LUCENE_TYPE_MAP = ImmutableMap.<DataType, SortField.Type>builder()
            .put(DataTypes.BOOLEAN, SortField.Type.STRING)
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

    public static class SortSymbolContext {

        private final boolean reverseFlag;
        private final CollectorContext context;
        private final Boolean nullFirst;

        public SortSymbolContext(SearchContext searchContext, boolean reverseFlag, Boolean nullFirst) {
            this.nullFirst = nullFirst;
            this.context = new CollectorContext();
            this.context.searchContext(searchContext);
            this.reverseFlag = reverseFlag;
        }
    }

    private final CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor;

    public SortSymbolVisitor(CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor) {
        super();
        this.inputSymbolVisitor = inputSymbolVisitor;
    }

    public SortField[] generateSortFields(List<Symbol> sortSymbols,
                                          SearchContext searchContext,
                                          boolean[] reverseFlags,
                                          Boolean[] nullsFirst) {
        SortField[] sortFields = new SortField[sortSymbols.size()];
        for (int i = 0; i < sortSymbols.size(); i++) {
            Symbol sortSymbol = sortSymbols.get(i);
            sortFields[i] = generateSortField(sortSymbol, new SortSymbolContext(searchContext, reverseFlags[i], nullsFirst[i]));
        }
        return sortFields;
    }

    public SortField generateSortField(Symbol symbol, SortSymbolContext sortSymbolContext) {
        return process(symbol, sortSymbolContext);
    }


    /**
     * generate a SortField from a Reference symbol.
     *
     * the implementation is similar to what {@link org.elasticsearch.search.sort.SortParseElement}
     * does.
     */
    @Override
    public SortField visitReference(final Reference symbol, final SortSymbolContext context) {
        // can't use the SortField(fieldName, type) constructor
        // because values are saved using docValues and therefore they're indexed in lucene as binary and not
        // with the reference valueType.
        // this is why we use a custom comparator source with the same logic as ES

        ColumnIdent columnIdent = symbol.info().ident().columnIdent();
        if (columnIdent.isColumn() && SortParseElement.SCORE_FIELD_NAME.equals(columnIdent.name())) {
            return !context.reverseFlag ? SortParseElement.SORT_SCORE_REVERSE : SortParseElement.SORT_SCORE;
        }

        MultiValueMode sortMode = context.reverseFlag ? MultiValueMode.MAX : MultiValueMode.MIN;
        SearchContext searchContext = context.context.searchContext();

        String indexName;
        IndexFieldData.XFieldComparatorSource fieldComparatorSource;
        FieldMapper fieldMapper = context.context.searchContext().smartNameFieldMapper(columnIdent.fqn());
        if (fieldMapper == null){
            indexName = columnIdent.fqn();
            fieldComparatorSource = new NullFieldComparatorSource(LUCENE_TYPE_MAP.get(symbol.valueType()), context.reverseFlag, context.nullFirst);
        } else {
            indexName = fieldMapper.names().indexName();
            fieldComparatorSource = searchContext.fieldData()
                    .getForField(fieldMapper)
                    .comparatorSource(SortOrder.missing(context.reverseFlag, context.nullFirst), sortMode, null);
        }
        return new SortField(
                indexName,
                fieldComparatorSource,
                context.reverseFlag
        );
    }

    @Override
    public SortField visitFunction(final Function function, final SortSymbolContext context) {
        CollectInputSymbolVisitor.Context inputContext = inputSymbolVisitor.process(function);
        ArrayList<Input<?>> inputs = inputContext.topLevelInputs();
        assert inputs.size() == 1;
        final Input functionInput = inputs.get(0);
        @SuppressWarnings("unchecked")
        final List<LuceneCollectorExpression> expressions = inputContext.docLevelExpressions();
        // our boolean functions return booleans, no BytesRefs, handle them differently
        // this is a hack, but that is how it worked before, so who cares :)
        final SortField.Type type = function.valueType().equals(DataTypes.BOOLEAN) ? null : LUCENE_TYPE_MAP.get(function.valueType());
        final SortField.Type reducedType = MoreObjects.firstNonNull(type, SortField.Type.DOC);

        return new SortField(function.toString(), new IndexFieldData.XFieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String fieldName, int numHits, int sortPos, boolean reversed) throws IOException {
                return new InputFieldComparator(
                        numHits,
                        context.context,
                        expressions,
                        functionInput,
                        function.valueType(),
                        type == null ? null : missingObject(SortOrder.missing(context.reverseFlag, context.nullFirst), reversed)
                );
            }

            @Override
            public SortField.Type reducedType() {
                return reducedType;
            }
        }, context.reverseFlag);
    }

    @Override
    protected SortField visitSymbol(Symbol symbol, SortSymbolContext context) {
        throw new UnsupportedOperationException(
                SymbolFormatter.format("sorting on %s is not supported", symbol));
    }
}
