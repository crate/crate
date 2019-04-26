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
import io.crate.analyze.relations.JoinPair;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Path;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MultiSourceSelect implements AnalyzedRelation {

    private final Map<QualifiedName, AnalyzedRelation> sources;
    private final Fields fields;
    private final List<JoinPair> joinPairs;
    private final boolean isDistinct;
    private final QuerySpec querySpec;
    private QualifiedName qualifiedName;

    public MultiSourceSelect(boolean isDistinct,
                             Map<QualifiedName, AnalyzedRelation> sources,
                             Collection<? extends Path> outputNames,
                             QuerySpec querySpec,
                             List<JoinPair> joinPairs) {
        this.isDistinct = isDistinct;
        assert sources.size() > 1 : "MultiSourceSelect requires at least 2 relations";
        this.qualifiedName = generateName(sources.keySet());
        this.sources = sources;
        for (Map.Entry<QualifiedName, AnalyzedRelation> entry : sources.entrySet()) {
            entry.getValue().setQualifiedName(entry.getKey());
        }
        this.querySpec = querySpec;
        this.joinPairs = joinPairs;
        assert outputNames.size() == querySpec.outputs().size() : "size of outputNames and outputSymbols must match";
        fields = new Fields(outputNames.size());
        Iterator<Symbol> outputsIterator = querySpec.outputs().iterator();
        for (Path path : outputNames) {
            fields.add(path, new Field(this, path, outputsIterator.next().valueType()));
        }
    }

    private static QualifiedName generateName(Set<QualifiedName> sourceNames) {
        StringBuilder sb = new StringBuilder();
        sb.append("J.");
        for (QualifiedName sourceName : sourceNames) {
            sb.append(sourceName);
        }
        return new QualifiedName(sb.toString());
    }

    public Map<QualifiedName, AnalyzedRelation> sources() {
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
    public boolean isDistinct() {
        return isDistinct;
    }

    @Override
    public String toString() {
        return "MSS{" + sources.keySet() + '}';
    }
}
