/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.sql.tree.QualifiedName;

public class SourceRelation {

    private final QualifiedName qualifiedName;
    private final AnalyzedRelation relation;
    private QuerySpec querySpec;

    public SourceRelation(QualifiedName qualifiedName, AnalyzedRelation relation) {
        this(qualifiedName, relation, null);
    }

    public SourceRelation(QualifiedName qualifiedName, AnalyzedRelation relation, QuerySpec querySpec) {
        this.qualifiedName = qualifiedName;
        this.relation = relation;
        this.querySpec = querySpec;
    }

    public QualifiedName qualifiedName() {
        return qualifiedName;
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
               "qName=" + qualifiedName +
               ", rel=" + relation +
               ", qs=" + querySpec +
               '}';
    }
}
