/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.eval;

import java.util.HashSet;
import java.util.Set;

import io.crate.expression.scalar.Ignore3vlFunction;
import io.crate.expression.scalar.cast.ExplicitCastFunction;
import io.crate.expression.scalar.cast.ImplicitCastFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.types.DataTypes;

public class NullabilityVisitor extends SymbolVisitor<NullabilityVisitor.NullabilityContext, Void> {

    private final Set<String> CAST_FUNCTIONS = Set.of(ImplicitCastFunction.NAME, ExplicitCastFunction.NAME);

    @Override
    public Void visitReference(Reference symbol, NullabilityContext context) {
        // if ref is nullable and all its parents are nullable
        if (symbol.isNullable() && context.isNullable) {
            context.collectNullableReferences(symbol);
        }
        return null;
    }

    @Override
    public Void visitFunction(Function function, NullabilityContext context) {
        String functionName = function.name();
        if (CAST_FUNCTIONS.contains(functionName)) {
            // Cast functions should be ignored except for the case where the incoming
            // datatype is an object. There we need to exclude null values to not match
            // empty objects on the query
            var a = function.arguments().get(0);
            var b = function.arguments().get(1);
            if (a instanceof Reference ref && b instanceof Literal<?>) {
                if (ref.valueType().id() == DataTypes.UNTYPED_OBJECT.id()) {
                    return null;
                }
            }
        } else if (Ignore3vlFunction.NAME.equals(functionName)) {
            context.isNullable = false;
            return null;
        } else {
            var signature = function.signature();
            if (signature.hasFeature(Scalar.Feature.NON_NULLABLE)) {
                context.isNullable = false;
            } else if (!signature.hasFeature(Scalar.Feature.NULLABLE)) {
                // default case
                context.enforceThreeValuedLogic = true;
                return null;
            }
        }
        // saves and restores isNullable of the current context
        // such that any non-nullables observed from the left arg is not transferred to the right arg.
        boolean isNullable = context.isNullable;
        for (Symbol arg : function.arguments()) {
            arg.accept(this, context);
            context.isNullable = isNullable;
        }
        return null;
    }

    public static class NullabilityContext {
        private final HashSet<Reference> nullableReferences = new HashSet<>();
        private boolean isNullable = true;
        private boolean enforceThreeValuedLogic = false;

        void collectNullableReferences(Reference symbol) {
            nullableReferences.add(symbol);
        }

        public Set<Reference> nullableReferences() {
            return nullableReferences;
        }

        public boolean enforceThreeValuedLogic() {
            return enforceThreeValuedLogic;
        }
    }
}
