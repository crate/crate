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
import io.crate.analyze.symbol.*;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.executor.transport.task.elasticsearch.SortOrder;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.SortParseElement;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SortSymbolVisitor extends SymbolVisitor<SortSymbolVisitor.SortSymbolContext, SortField> {

    public static final SortField SORT_SCORE_REVERSE = new SortField(null, SortField.Type.SCORE, true);

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

        public SortSymbolContext(CollectorContext collectorContext, boolean reverseFlag, Boolean nullFirst) {
            this.nullFirst = nullFirst;
            this.context = collectorContext;
            this.reverseFlag = reverseFlag;
        }
    }

    private final CollectInputSymbolVisitor<?> inputSymbolVisitor;

    public SortSymbolVisitor(CollectInputSymbolVisitor<?> inputSymbolVisitor) {
        super();
        this.inputSymbolVisitor = inputSymbolVisitor;
    }

    public SortField[] generateSortFields(List<Symbol> sortSymbols,
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

        if (columnIdent.isColumn()) {
            if (SortParseElement.SCORE_FIELD_NAME.equals(columnIdent.name())) {
                return !context.reverseFlag ? SORT_SCORE_REVERSE : SortParseElement.SORT_SCORE;
            } else if (DocSysColumns.RAW.equals(columnIdent) || DocSysColumns.ID.equals(columnIdent)) {
                return customSortField(DocSysColumns.nameForLucene(columnIdent), symbol, context,
                        LUCENE_TYPE_MAP.get(symbol.valueType()), false);
            }
        }

        MultiValueMode sortMode = context.reverseFlag ? MultiValueMode.MAX : MultiValueMode.MIN;

        String indexName;
        IndexFieldData.XFieldComparatorSource fieldComparatorSource;
        MappedFieldType fieldType = context.context.mapperService().smartNameFieldType(columnIdent.fqn());
        if (fieldType == null){
            indexName = columnIdent.fqn();
            fieldComparatorSource = new NullFieldComparatorSource(LUCENE_TYPE_MAP.get(symbol.valueType()), context.reverseFlag, context.nullFirst);
        } else {
            indexName = fieldType.names().indexName();
            fieldComparatorSource = context.context.fieldData()
                    .getForField(fieldType)
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
        // our boolean functions return booleans, no BytesRefs, handle them differently
        // this is a hack, but that is how it worked before, so who cares :)
        SortField.Type type = function.valueType().equals(DataTypes.BOOLEAN) ? null : LUCENE_TYPE_MAP.get(function.valueType());
        SortField.Type reducedType = MoreObjects.firstNonNull(type, SortField.Type.DOC);
        return customSortField(function.toString(), function, context, reducedType, type == null);
    }

    @Override
    protected SortField visitSymbol(Symbol symbol, SortSymbolContext context) {
        throw new UnsupportedOperationException(
                SymbolFormatter.format("sorting on %s is not supported", symbol));
    }

    private SortField customSortField(String name,
                                      final Symbol symbol,
                                      final SortSymbolContext context,
                                      final SortField.Type reducedType,
                                      final boolean missingNullValue) {
        CollectInputSymbolVisitor.Context inputContext = inputSymbolVisitor.extractImplementations(symbol);
        List<Input<?>> inputs = inputContext.topLevelInputs();
        assert inputs.size() == 1;
        final Input input = inputs.get(0);
        @SuppressWarnings("unchecked")
        final List<LuceneCollectorExpression> expressions = inputContext.docLevelExpressions();

        return new SortField(name, new IndexFieldData.XFieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String fieldName, int numHits, int sortPos, boolean reversed) throws IOException {
                for (LuceneCollectorExpression collectorExpression : expressions) {
                    collectorExpression.startCollect(context.context);
                }
                if (context.context.visitor().required()) {
                    return new FieldsVisitorInputFieldComparator(
                            numHits,
                            context.context.visitor(),
                            expressions,
                            input,
                            symbol.valueType(),
                            missingNullValue ? null : missingObject(SortOrder.missing(context.reverseFlag, context.nullFirst), reversed)
                    );

                } else {
                    return new InputFieldComparator(
                            numHits,
                            expressions,
                            input,
                            symbol.valueType(),
                            missingNullValue ? null : missingObject(SortOrder.missing(context.reverseFlag, context.nullFirst), reversed)
                    );
                }
            }

            @Override
            public SortField.Type reducedType() {
                return reducedType;
            }
        }, context.reverseFlag);
    }
}
