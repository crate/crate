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

    private ParentRelations() {
        sourcesTree = Collections.emptyList();
    }

    private ParentRelations(ArrayList<Map<RelationName, AnalyzedRelation>> sourcesTree) {
        this.sourcesTree = sourcesTree;
    }

    public ParentRelations newLevel(Map<RelationName, AnalyzedRelation> sources) {
        ArrayList<Map<RelationName, AnalyzedRelation>> newSourcesTree = new ArrayList<>(sourcesTree);
        newSourcesTree.add(sources);
        return new ParentRelations(newSourcesTree);
    }

    public boolean containsRelation(RelationName qualifiedName) {
        return getAncestor(qualifiedName) != null;
    }

    @Nullable
    public AnalyzedRelation getParent(RelationName relationName) {
        if (sourcesTree.isEmpty() || sourcesTree.size() < 2) {
            return null;
        }
        // the last item is the _current_ relation, need one before that for the immediate parent
        Map<RelationName, AnalyzedRelation> parent = sourcesTree.get(sourcesTree.size() - 2);
        return parent.get(relationName);
    }

    public Iterable<AnalyzedRelation> getParents() {
        if (sourcesTree.isEmpty() || sourcesTree.size() < 2) {
            return Collections.emptyList();
        }
        return sourcesTree.get(sourcesTree.size() - 2).values();
    }

    @Nullable
    public AnalyzedRelation getAncestor(RelationName relationName) {
        AnalyzedRelation relation = null;
        for (int i = sourcesTree.size() - 1; i >= 0; i--) {
            relation = sourcesTree.get(i).get(relationName);
            if (relation != null) {
                break;
            }
        }
        return relation;
    }
}
