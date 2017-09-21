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

package io.crate.planner.consumer;

import io.crate.analyze.symbol.DefaultTraversalSymbolVisitor;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.MatchPredicate;
import io.crate.sql.tree.QualifiedName;

import java.util.Set;

public class QualifiedNameCollector extends DefaultTraversalSymbolVisitor<Set<QualifiedName>, Void> {

    public static final QualifiedNameCollector INSTANCE = new QualifiedNameCollector();

    @Override
    public Void visitField(Field field, Set<QualifiedName> context) {
        context.add(field.relation().getQualifiedName());
        return null;
    }

    @Override
    public Void visitMatchPredicate(MatchPredicate matchPredicate, Set<QualifiedName> context) {
        for (Field field : matchPredicate.identBoostMap().keySet()) {
            context.add(field.relation().getQualifiedName());
        }
        return null;
    }
}
