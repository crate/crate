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

package io.crate.analyze;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.RelationSplitter;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.OutputName;
import io.crate.metadata.Path;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.common.Nullable;

import java.util.*;

public class MultiSourceSelect implements QueriedRelation {

    public static class Source {
        private final AnalyzedRelation relation;
        private final QuerySpec querySpec;

        public Source(AnalyzedRelation relation, QuerySpec querySpec) {
            this.relation = relation;
            this.querySpec = querySpec;
        }

        public QuerySpec querySpec() {
            return querySpec;
        }

        public AnalyzedRelation relation() {
            return relation;
        }
    }

    private final HashMap<QualifiedName, Source> sources;
    private final List<Field> fields;
    private final QuerySpec querySpec;
    private final Set<Symbol> requiredForQuery;
    private final OrderBy remainingOrderBy;

    public MultiSourceSelect(
            Map<QualifiedName, AnalyzedRelation> sources,
            List<OutputName> outputNames,
            QuerySpec querySpec) {
        assert sources.size() > 1 : "MultiSourceSelect requires at least 2 relations";
        this.querySpec = querySpec;
        this.sources = new HashMap<>(sources.size());
        RelationSplitter splitter = initSources(sources);
        remainingOrderBy = splitter.remainingOrderBy();
        requiredForQuery = splitter.requiredForQuery();
        assert outputNames.size() == querySpec.outputs().size() : "size of outputNames and outputSymbols must match";
        fields = new ArrayList<>(outputNames.size());
        for (int i = 0; i < outputNames.size(); i++) {
            fields.add(new Field(this, outputNames.get(i), querySpec.outputs().get(i).valueType()));
        }
    }

    public Set<Symbol> requiredForQuery() {
        return requiredForQuery;
    }

    public Map<QualifiedName, Source> sources() {
        return sources;
    }

    public void normalize(EvaluatingNormalizer normalizer) {
        querySpec.normalize(normalizer);
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitMultiSourceSelect(this, context);
    }

    @Override
    public Field getField(Path path) {
        throw new UnsupportedOperationException("getField on SelectAnalyzedStatement is not implemented");
    }

    @Override
    public Field getWritableField(Path path) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("SelectAnalyzedStatement is not writable");
    }

    @Override
    public List<Field> fields() {
        return fields;
    }

    public QuerySpec querySpec() {
        return querySpec;
    }

    private RelationSplitter initSources(Map<QualifiedName, AnalyzedRelation> sources) {

        RelationSplitter splitter = RelationSplitter.process(querySpec, sources.values());
        for (Map.Entry<QualifiedName, AnalyzedRelation> entry : sources.entrySet()) {
            QuerySpec spec = splitter.getSpec(entry.getValue());
            assert spec != null;
            Source source = new Source(entry.getValue(), spec);
            this.sources.put(entry.getKey(), source);
        }
        return splitter;
    }

    /**
     * Returns an orderBy containing only orderings which are not already pushed down to sources.
     * This method always returns null if no orderBySymbols are left. The existing orderBy is returned if it
     * there are no changes.
     */
    @Nullable
    public OrderBy remainingOrderBy() {
        return remainingOrderBy;
    }
}
