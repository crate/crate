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

import com.google.common.base.Optional;
import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Path;
import io.crate.metadata.table.Operation;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.tree.QualifiedName;

import java.util.*;

public class MultiSourceSelect implements QueriedRelation {

    private final RelationSplitter splitter;
    private final HashMap<QualifiedName, Source> sources;
    private final QuerySpec querySpec;
    private final Fields fields;
    private final List<JoinPair> joinPairs;
    private QualifiedName qualifiedName;

    public MultiSourceSelect(Map<QualifiedName, AnalyzedRelation> sources,
                             Collection<? extends Path> outputNames,
                             QuerySpec querySpec,
                             List<JoinPair> joinPairs) {
        assert sources.size() > 1 : "MultiSourceSelect requires at least 2 relations";
        this.splitter = new RelationSplitter(querySpec, sources.values());
        this.sources = initializeSources(sources);
        this.querySpec = querySpec;
        this.joinPairs = joinPairs;
        assert outputNames.size() == querySpec.outputs().size() : "size of outputNames and outputSymbols must match";
        fields = new Fields(outputNames.size());
        Iterator<Symbol> outputsIterator = querySpec.outputs().iterator();
        for (Path path : outputNames) {
            fields.add(path, new Field(this, path, outputsIterator.next().valueType()));
        }
    }

    public Set<Symbol> requiredForQuery() {
        return splitter.requiredForQuery();
    }

    public Set<Field> canBeFetched(){
        return splitter.canBeFetched();
    }

    public Map<QualifiedName, Source> sources() {
        return sources;
    }

    public List<JoinPair> joinPairs() {
        return joinPairs;
    }

    public JoinType joinTypeForRelations(QualifiedName left, QualifiedName right) {
        JoinType joinType = JoinPair.joinTypeForRelations(left, right, joinPairs);
        if (joinType == null) {
            // default to cross join (or inner, doesn't matter)
            return JoinType.CROSS;
        }
        return joinType;
    }

    public void rewriteNamesOfJoinPairs(QualifiedName left, QualifiedName right, QualifiedName newName) {
        for (JoinPair joinPair : joinPairs) {
            joinPair.replaceNames(left, right, newName);
        }
    }

    public void addImplicitInnerJoinPairs(Set<Set<QualifiedName>> innerJoinPairs) {
        for (Set<QualifiedName> relationPairs : innerJoinPairs) {
            assert relationPairs.size() == 2 : "relation pair is not a valid pair (size is not 2)";
            Iterator<QualifiedName> it = relationPairs.iterator();
            QualifiedName left = it.next();
            QualifiedName right = it.next();
            if (JoinPair.joinTypeForRelations(left, right, joinPairs) == null) {
                joinPairs.add(new JoinPair(left, right, JoinType.INNER));
            }
        }
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitMultiSourceSelect(this, context);
    }

    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException {
        if (operation != Operation.READ) {
            throw new UnsupportedOperationException("getField on MultiSourceSelect is only supported for READ operations");
        }
        return fields.get(path);
    }

    @Override
    public List<Field> fields() {
        return fields.asList();
    }

    @Override
    public QualifiedName getQualifiedName() {
        return qualifiedName;
    }

    @Override
    public void setQualifiedName(QualifiedName qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    public QuerySpec querySpec() {
        return querySpec;
    }

    private static HashMap<QualifiedName, Source> initializeSources(Map<QualifiedName, AnalyzedRelation> originalSources) {
        HashMap<QualifiedName, Source> sources = new LinkedHashMap<>(originalSources.size());
        for (Map.Entry<QualifiedName, AnalyzedRelation> entry : originalSources.entrySet()) {
            Source source = new Source(entry.getValue());
            sources.put(entry.getKey(), source);
        }
        return sources;
    }

    public void pushDownQuerySpecs() {
        splitter.process();
        for (Source source : sources.values()) {
            QuerySpec spec = splitter.getSpec(source.relation());
            source.querySpec(spec);
        }
    }

    public Optional<RemainingOrderBy> remainingOrderBy() {
        return splitter.remainingOrderBy();
    }

    public static class Source {
        private final AnalyzedRelation relation;
        private QuerySpec querySpec;

        public Source(AnalyzedRelation relation) {
            this(relation, null);
        }

        public Source(AnalyzedRelation relation, QuerySpec querySpec) {
            this.relation = relation;
            this.querySpec = querySpec;
        }

        public QuerySpec querySpec() {
            return querySpec;
        }

        public void querySpec(QuerySpec querySpec) {
            this.querySpec = querySpec;
        }

        public AnalyzedRelation relation() {
            return relation;
        }

        @Override
        public String toString() {
            return "Source{" +
                   "rel=" + relation +
                   ", qs=" + querySpec +
                   '}';
        }
    }
}
