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
import com.google.common.collect.ImmutableList;
import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Path;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import java.util.*;

public class MultiSourceSelect implements QueriedRelation {

    private final byte relationId;
    private final RelationSplitter splitter;
    private final HashMap<QualifiedName, RelationSource> sources;
    private final QuerySpec querySpec;
    private final Fields fields;
    private final List<JoinPair> joinPairs;
    private final List<Symbol> outputSymbols;
    private QualifiedName qualifiedName;

    public MultiSourceSelect(byte relationId,
                             Map<QualifiedName, AnalyzedRelation> sources,
                             List<Symbol> outputSymbols,
                             Collection<? extends Path> outputNames,
                             QuerySpec querySpec,
                             List<JoinPair> joinPairs) {
        this.relationId = relationId;
        this.outputSymbols = ImmutableList.copyOf(outputSymbols);
        assert sources.size() > 1 : "MultiSourceSelect requires at least 2 relations";
        this.splitter = new RelationSplitter(querySpec, sources.values(), joinPairs);
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

    public Set<Field> canBeFetched() {
        return splitter.canBeFetched();
    }

    public Map<QualifiedName, RelationSource> sources() {
        return sources;
    }

    public List<JoinPair> joinPairs() {
        return joinPairs;
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
    public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    @Override
    public QuerySpec querySpec() {
        return querySpec;
    }

    @Override
    public byte relationId() {
        return relationId;
    }

    private static HashMap<QualifiedName, RelationSource> initializeSources(Map<QualifiedName, AnalyzedRelation> originalSources) {
        HashMap<QualifiedName, RelationSource> sources = new LinkedHashMap<>(originalSources.size());
        for (Map.Entry<QualifiedName, AnalyzedRelation> entry : originalSources.entrySet()) {
            RelationSource source = new RelationSource(entry.getKey(), entry.getValue());
            sources.put(entry.getKey(), source);
        }
        return sources;
    }

    public void pushDownQuerySpecs() {
        splitter.process();
        for (RelationSource source : sources.values()) {
            QuerySpec spec = splitter.getSpec(source.relation());
            source.querySpec(spec);
        }
    }

    public Optional<RemainingOrderBy> remainingOrderBy() {
        return splitter.remainingOrderBy();
    }

    public List<Symbol> outputSymbols() {
        return outputSymbols;
    }
}
