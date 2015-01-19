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

package io.crate.analyze.relations;

import io.crate.sql.tree.QualifiedName;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RelationAnalysisContext {

    private Map<QualifiedName, AnalyzedRelation> sources = new HashMap<>();

    public RelationAnalysisContext() {
    }

    public void addSourceRelation(String nameOrAlias, AnalyzedRelation relation) {
        sources.put(new QualifiedName(nameOrAlias), relation);
    }

    public void addSourceRelation(String schemaName, String nameOrAlias, AnalyzedRelation relation) {
        sources.put(new QualifiedName(Arrays.asList(schemaName, nameOrAlias)), relation);
    }

    public Map<QualifiedName, AnalyzedRelation> sources() {
        return sources;
    }
}
