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

package io.crate.planner.fetch;

import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import io.crate.Constants;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.FetchReference;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;

import javax.annotation.Nullable;
import java.util.*;

public class FetchPushDown {

    private final QuerySpec querySpec;
    private final DocTableRelation docTableRelation;
    private Field docIdField;
    private LinkedHashMap<ReferenceIdent, FetchReference> fetchReferences;

    public FetchPushDown(QuerySpec querySpec, DocTableRelation docTableRelation) {
        this.querySpec = querySpec;
        this.docTableRelation = docTableRelation;
    }

    public Field docIdField() {
        return docIdField;
    }

    public Collection<Reference> fetchRefs() {
        if (fetchReferences == null || fetchReferences.isEmpty()) {
            return ImmutableList.of();
        }
        return Collections2.transform(fetchReferences.values(), FetchReference.REF_FUNCTION);
    }

    private FetchReference allocateFetchReference(Reference ref) {
        assert ref.info().granularity() == RowGranularity.DOC;
        if (!ref.ident().columnIdent().isSystemColumn()) {
            ref = DocReferenceConverter.toSourceLookup(ref);
        }
        if (fetchReferences == null) {
            fetchReferences = new LinkedHashMap<>();
        }
        FetchReference fRef = fetchReferences.get(ref.ident());
        if (fRef == null) {
            fRef = new FetchReference(docIdField, ref);
            fetchReferences.put(ref.ident(), fRef);
        }
        return fRef;
    }

    @Nullable
    public QueriedDocTable pushDown() {
        assert !querySpec.groupBy().isPresent() && !querySpec.having().isPresent() && !querySpec.hasAggregates();

        Optional<OrderBy> orderBy = querySpec.orderBy();

        FetchRequiredVisitor.Context context;
        if (orderBy.isPresent()){
            context = new FetchRequiredVisitor.Context(new LinkedHashSet<>(querySpec.orderBy().get().orderBySymbols()));
        } else {
            context = new FetchRequiredVisitor.Context();

        }

        boolean fetchRequired = FetchRequiredVisitor.INSTANCE.process(querySpec.outputs(), context);
        if (!fetchRequired) return null;

        // build the subquery
        QuerySpec sub = new QuerySpec();
        Reference docIdReference = new Reference(DocSysColumns.forTable(docTableRelation.tableInfo().ident(), DocSysColumns.DOCID));

        List<Symbol> outputs = new ArrayList<>();
        if (orderBy.isPresent()) {
            sub.orderBy(orderBy.get());
            outputs.add(docIdReference);
            outputs.addAll(context.querySymbols());
        } else {
            outputs.add(docIdReference);
        }
        for (Symbol symbol : querySpec.outputs()) {
            if (ScoreReferenceDetector.detect(symbol) && !outputs.contains(symbol)) {
                outputs.add(symbol);
            }
        }
        sub.outputs(outputs);
        QueriedDocTable subRelation = new QueriedDocTable(docTableRelation, sub);
        List<Field> fields = subRelation.fields();
        HashMap<Symbol, Field> fieldMap = new HashMap<>(sub.outputs().size());

        Iterator<Field> iFields = fields.iterator();
        for (Symbol symbol : sub.outputs()) {
            fieldMap.put(symbol, iFields.next());
        }

        // push down the where clause
        sub.where(querySpec.where());
        querySpec.where(null);

        // replace output symbols with fields or fetch references
        docIdField = fields.get(0);

        ToFetchReferenceVisitor toFetchReferenceVisitor = new ToFetchReferenceVisitor(fieldMap);
        toFetchReferenceVisitor.processInplace(querySpec.outputs(), null);

        if (orderBy.isPresent()) {
            // replace order by symbols with fields, we need to copy the order by since it was pushed down to the
            // subquery before
            ArrayList<Symbol> newOrderBySymbols = new ArrayList<>(orderBy.get().orderBySymbols().size());
            for (Symbol symbol : orderBy.get().orderBySymbols()) {
                Field queryField = fieldMap.get(symbol);
                assert queryField != null;
                newOrderBySymbols.add(queryField);
            }
            querySpec.orderBy(new OrderBy(newOrderBySymbols, orderBy.get().reverseFlags(), orderBy.get().nullsFirst()));
        }

        sub.limit(querySpec.limit().or(Constants.DEFAULT_SELECT_LIMIT) + querySpec.offset());
        return subRelation;
    }

    private class ToFetchReferenceVisitor extends ReplacingSymbolVisitor<Void> {

        private final Map<Symbol, Field> fieldMap;

        private ToFetchReferenceVisitor(Map<Symbol, Field> fieldMap) {
            super(false);
            this.fieldMap = fieldMap;
        }

        @Override
        public Symbol process(Symbol symbol, @Nullable Void context) {
            Field field = fieldMap.get(symbol);
            if (field != null) {
                return field;
            }
            return super.process(symbol, context);
        }

        @Override
        public Symbol visitReference(Reference ref, Void context) {
            Field field = fieldMap.get(ref);
            if (field != null) {
                return field;
            }
            if (ref.info().granularity() == RowGranularity.PARTITION) {
                return ref;
            }
            return allocateFetchReference(ref);
        }

    }

}
