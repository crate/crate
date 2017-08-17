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

package io.crate.analyze.symbol;

public final class FuncSymbols {


    /**
     * @return a function that applies the mapper to all "nodes" (=functions) in a symbol
     */
    public static java.util.function.Function<? super Symbol, ? extends Symbol> mapNodes(java.util.function.Function<? super Function, ? extends Symbol> mapper) {
        return st -> mapNodes(st, mapper);
    }

    /**
     * Apply the mapper function on all nodes in a symbolTree.
     */
    public static Symbol mapNodes(Symbol st, java.util.function.Function<? super Function, ? extends Symbol> mapper) {
        return Visitor.INSTANCE.process(st, mapper);
    }

    private static class Visitor extends FunctionCopyVisitor<java.util.function.Function<? super Function, ? extends Symbol>> {

        private static final Visitor INSTANCE = new Visitor();

        @Override
        public Symbol visitFunction(Function func, java.util.function.Function<? super Function, ? extends Symbol> mapper) {
            return mapper.apply(processAndMaybeCopy(func, mapper));
        }
    }
}
