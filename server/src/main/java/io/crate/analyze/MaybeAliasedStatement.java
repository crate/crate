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

package io.crate.analyze;

import java.util.function.Function;

import io.crate.analyze.relations.AliasedAnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.FieldResolver;
import io.crate.expression.symbol.FieldReplacer;
import io.crate.expression.symbol.Symbol;

/**
 * A helper class to unwrap a possible {@link AliasedAnalyzedRelation} relation and re-map all fields
 * to the child relation. This is necessary as the fields will be resolved by a {@link FieldResolver} which isn't
 * implemented by a generic aliased relation but by the child relation and thus fields must be re-mapped to point to
 * the child's relation (== use every fields pointer) instead.
 */
public class MaybeAliasedStatement {

    public static MaybeAliasedStatement analyze(AnalyzedRelation relation) {
        if (relation instanceof AliasedAnalyzedRelation aliasedAnalyzedRelation) {
            return new MaybeAliasedStatement(
                aliasedAnalyzedRelation.relation(),
                FieldReplacer.bind(aliasedAnalyzedRelation::resolveField)
            );
        }
        return new MaybeAliasedStatement(relation, s -> s);
    }

    private final AnalyzedRelation relation;
    private final Function<? super Symbol, ? extends Symbol> mapper;

    private MaybeAliasedStatement(AnalyzedRelation relation,
                                  Function<? super Symbol, ? extends Symbol> mapper) {
        this.relation = relation;
        this.mapper = mapper;
    }

    AnalyzedRelation nonAliasedRelation() {
        return relation;
    }

    Symbol maybeMapFields(Symbol symbol) {
        return mapper.apply(symbol);
    }
}
