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
import com.google.common.collect.ImmutableList;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.*;
import io.crate.metadata.DocReferenceConverter;
import io.crate.metadata.Reference;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.*;

/**
 * See {@link #pushDown()} for explanation what this class does.
 */
public class FetchPushDown {

    private final QuerySpec querySpec;
    private final DocTableRelation docTableRelation;
    private final byte relationId;
    private static final InputColumn FETCHID_COL = new InputColumn(0, DataTypes.LONG);
    private LinkedHashMap<Reference, FetchReference> fetchRefs;
    private LinkedHashMap<Reference, FetchReference> partitionRefs;

    public FetchPushDown(QuerySpec querySpec, DocTableRelation docTableRelation, byte relationId) {
        this.querySpec = querySpec;
        this.docTableRelation = docTableRelation;
        this.relationId = relationId;
    }

    public InputColumn fetchIdCol() {
        return FETCHID_COL;
    }

    public Collection<Reference> fetchRefs() {
        if (fetchRefs == null || fetchRefs.isEmpty()) {
            return ImmutableList.of();
        }
        return fetchRefs.keySet();
    }

    private FetchReference allocateReference(Reference ref) {
        RowGranularity granularity = ref.granularity();
        if (granularity == RowGranularity.DOC) {
            ref = DocReferenceConverter.toSourceLookup(ref);
            if (fetchRefs == null) {
                fetchRefs = new LinkedHashMap<>();
            }
            return toFetchReference(ref, fetchRefs);
        } else {
            assert docTableRelation.tableInfo().isPartitioned() && granularity == RowGranularity.PARTITION;
            if (partitionRefs == null) {
                partitionRefs = new LinkedHashMap<>(docTableRelation.tableInfo().partitionedBy().size());
            }
            return toFetchReference(ref, partitionRefs);
        }
    }

    private FetchReference toFetchReference(Reference ref, LinkedHashMap<Reference, FetchReference> refs) {
        FetchReference fRef = refs.get(ref);
        if (fRef == null) {
            fRef = new FetchReference(FETCHID_COL, ref);
            refs.put(ref, fRef);
        }
        return fRef;
    }

    /**
     * Create a new relation that has all columns which are only relevant for the final result replaced with a fetchid.
     *
     * If values are only relevant for the final result the retrieval can be delayed until the last possible moment.
     * This should usually be used to avoid retrieving intermediate values which might be discarded.
     *
     * <p>
     *     Example
     * </p>
     * <pre>
     *     select a, b, c from t order by a limit 10
     *
     *     Becomes
     *
     *     select _fetchid, a from t order by a limit 10    <-- executed per node
     *                                                          Result is num_nodes * limit
     *                                                          Order by value is included because it's necessary
     *                                                          to do ordered merge
     *
     *     apply limit 10                                   <-- once on handler via topN
     *                                                          (num_nodes * limit - limit) rows are discarded
     *     fetch columns from nodes using _fetchids
     * </pre>
     */
    @Nullable
    public QueriedDocTable pushDown() {
        assert !querySpec.groupBy().isPresent() && !querySpec.having().isPresent() && !querySpec.hasAggregates()
            : "groupBy/having/aggregations must not be present. Aggregations need all values so fetch pushdown doesn't make sense";

        Optional<OrderBy> orderBy = querySpec.orderBy();

        FetchRequiredVisitor.Context context;
        if (orderBy.isPresent()) {
            context = new FetchRequiredVisitor.Context(new LinkedHashSet<>(orderBy.get().orderBySymbols()));
        } else {
            context = new FetchRequiredVisitor.Context();

        }

        boolean fetchRequired = FetchRequiredVisitor.INSTANCE.process(querySpec.outputs(), context);
        if (!fetchRequired) return null;

        // build the subquery
        QuerySpec sub = new QuerySpec();
        Reference fetchIdReference = DocSysColumns.forTable(docTableRelation.tableInfo().ident(), DocSysColumns.FETCHID);

        List<Symbol> outputs = new ArrayList<>();
        if (orderBy.isPresent()) {
            sub.orderBy(orderBy.get());
            outputs.add(fetchIdReference);
            outputs.addAll(context.querySymbols());
        } else {
            outputs.add(fetchIdReference);
        }
        for (Symbol symbol : querySpec.outputs()) {
            if (Symbols.containsColumn(symbol, DocSysColumns.SCORE) && !outputs.contains(symbol)) {
                outputs.add(symbol);
            }
        }
        sub.outputs(outputs);
        QueriedDocTable subRelation = new QueriedDocTable(relationId, docTableRelation, sub);
        HashMap<Symbol, InputColumn> symbolMap = new HashMap<>(sub.outputs().size());

        int index = 0;
        for (Symbol symbol : sub.outputs()) {
            symbolMap.put(symbol, new InputColumn(index++, symbol.valueType()));
        }

        // push down the where clause
        sub.where(querySpec.where());
        querySpec.where(null);

        ToFetchReferenceVisitor toFetchReferenceVisitor = new ToFetchReferenceVisitor();
        toFetchReferenceVisitor.processInplace(querySpec.outputs(), symbolMap);

        if (orderBy.isPresent()) {
            // replace order by symbols with input columns, we need to copy the order by since it was pushed down to the
            // subquery before
            List<Symbol> newOrderBySymbols = MappingSymbolVisitor.copying().process(orderBy.get().orderBySymbols(), symbolMap);
            querySpec.orderBy(new OrderBy(newOrderBySymbols, orderBy.get().reverseFlags(), orderBy.get().nullsFirst()));
        }
        sub.limit(querySpec.limit());
        sub.offset(querySpec.offset());
        return subRelation;
    }

    private class ToFetchReferenceVisitor extends MappingSymbolVisitor {

        private ToFetchReferenceVisitor() {
            super(ReplaceMode.COPY);
        }

        @Override
        public Symbol visitReference(Reference symbol, Map<? extends Symbol, ? extends Symbol> context) {
            Symbol mapped = context.get(symbol);
            if (mapped != null) {
                return mapped;
            }
            return allocateReference(symbol);
        }
    }

}
