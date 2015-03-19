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

package io.crate.metadata;

import com.google.common.base.Predicate;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.symbol.*;

import javax.annotation.Nullable;

public class ScoreReferenceDetector {

    private final static Visitor VISITOR = new Visitor();

    private final static Predicate<Reference> SCORE_PREDICATE = new Predicate<Reference>() {
        @Override
        public boolean apply(@Nullable Reference input) {
            assert input != null;

            ReferenceIdent ident = input.info().ident();
            if (ident.columnIdent().equals(DocSysColumns.SCORE)) {
                return true;
            }

            return false;
        }
    };

    public Boolean detect(Symbol symbol) {
        return VISITOR.process(symbol, SCORE_PREDICATE);
    }

    static class Visitor extends SymbolVisitor<Predicate<Reference>, Boolean> {
        @Override
        public Boolean visitFunction(Function symbol, Predicate<Reference> predicate) {
            for (Symbol argument : symbol.arguments()) {
                if (process(argument, predicate)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitDynamicReference(DynamicReference symbol, Predicate<Reference> predicate) {
            return visitReference(symbol, predicate);
        }

        @Override
        public Boolean visitReference(Reference symbol, Predicate<Reference> predicate) {
            return predicate.apply(symbol);
        }

        @Override
        protected Boolean visitSymbol(Symbol symbol, Predicate<Reference> predicate) {
            return false;
        }

    }
}
