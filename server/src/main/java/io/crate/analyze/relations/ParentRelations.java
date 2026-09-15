/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze.relations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import io.crate.metadata.RelationName;

public class ParentRelations {

    public static final ParentRelations NO_PARENTS = new ParentRelations();

    private final List<Map<RelationName, AnalyzedRelation>> sourcesTree;
    private final List<Map<RelationName, AnalyzedRelation>> withTree;

    private ParentRelations() {
        sourcesTree = Collections.emptyList();
        withTree = Collections.emptyList();
    }

    private ParentRelations(ArrayList<Map<RelationName, AnalyzedRelation>> sourcesTree,
                            ArrayList<Map<RelationName, AnalyzedRelation>> withTree) {
        this.sourcesTree = sourcesTree;
        this.withTree = withTree;
    }

    public ParentRelations newLevel(Map<RelationName, AnalyzedRelation> sources,
                                    Map<RelationName, AnalyzedRelation> withRelations) {
        ArrayList<Map<RelationName, AnalyzedRelation>> newSourcesTree = new ArrayList<>(sourcesTree.size() + 1);
        newSourcesTree.addAll(sourcesTree);
        newSourcesTree.add(sources);
        ArrayList<Map<RelationName, AnalyzedRelation>> newWithTree = new ArrayList<>(withTree.size() + 1);
        newWithTree.addAll(withTree);
        newWithTree.add(withRelations);
        return new ParentRelations(newSourcesTree, newWithTree);
    }

    public boolean containsRelation(RelationName qualifiedName) {
        return getAncestor(qualifiedName) != null;
    }

    public Iterable<AnalyzedRelation> getParents() {
        if (sourcesTree.isEmpty() || sourcesTree.size() < 2) {
            return Collections.emptyList();
        }
        return sourcesTree.get(sourcesTree.size() - 2).values();
    }

    @Nullable
    public AnalyzedRelation getAncestorWithQuery(RelationName relationName) {
        for (int i = withTree.size() - 1; i >= 0; i--) {
            AnalyzedRelation relation = withTree.get(i).get(relationName);
            if (relation != null) {
                return relation;
            }
        }
        return null;
    }

    @Nullable
    public AnalyzedRelation getAncestor(RelationName relationName) {
        for (int i = sourcesTree.size() - 1; i >= 0; i--) {
            AnalyzedRelation relation = sourcesTree.get(i).get(relationName);
            if (relation != null) {
                return relation;
            }
        }
        return null;
    }
}
