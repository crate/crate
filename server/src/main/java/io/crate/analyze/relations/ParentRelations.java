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

import io.crate.metadata.RelationName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
        for (int i = sourcesTree.size() - 1; i >= 0; i--) {
            if (sourcesTree.get(i).containsKey(qualifiedName)) {
                return true;
            }
        }
        return false;
    }
}
