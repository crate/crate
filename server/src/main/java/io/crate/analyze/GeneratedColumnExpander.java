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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.GtOperator;
import io.crate.expression.operator.GteOperator;
import io.crate.expression.operator.LtOperator;
import io.crate.expression.operator.LteOperator;
import io.crate.expression.operator.Operators;
import io.crate.expression.scalar.DateBinFunction;
import io.crate.expression.scalar.DateTruncFunction;
import io.crate.expression.scalar.arithmetic.CeilFunction;
import io.crate.expression.scalar.arithmetic.FloorFunction;
import io.crate.expression.scalar.arithmetic.RoundFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.FunctionCopyVisitor;
import io.crate.expression.symbol.RefReplacer;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.SearchPath;

public final class GeneratedColumnExpander {

    private static final Map<String, String> ROUNDING_FUNCTION_MAPPING = Map.of(
        GtOperator.NAME, GteOperator.NAME,
        LtOperator.NAME, LteOperator.NAME
    );

    private static final Set<String> ROUNDING_FUNCTIONS = Set.of(
        CeilFunction.CEIL,
        FloorFunction.NAME,
        RoundFunction.NAME,
        DateTruncFunction.NAME,
        DateBinFunction.NAME
    );

    private GeneratedColumnExpander() {
    }

    private static final ComparisonReplaceVisitor COMPARISON_REPLACE_VISITOR = new ComparisonReplaceVisitor();

    /**
     * @return symbol as is or rewritten to have generated columns expanded.
     *
     * <pre>
     *     example for an expansion:
     *
     *     generatedCols:       [day as date_trunc('day', ts)]
     *     expansionCandidates: [day]
     *
     *     input:   ts > $1
     *     output:  ts > $1 and day > date_trunc('day', ts)
     * </pre>
     */
    public static Symbol maybeExpand(Symbol symbol,
                                     List<GeneratedReference> generatedCols,
                                     List<Reference> expansionCandidates,
                                     NodeContext nodeCtx) {
        return COMPARISON_REPLACE_VISITOR.addComparisons(symbol, generatedCols, expansionCandidates, nodeCtx);
    }

    private static class ComparisonReplaceVisitor extends FunctionCopyVisitor<ComparisonReplaceVisitor.Context> {

        static class Context {
            private final HashMap<Reference, ArrayList<GeneratedReference>> referencedRefsToGeneratedColumn;
            private final NodeContext nodeCtx;

            public Context(HashMap<Reference, ArrayList<GeneratedReference>> referencedRefsToGeneratedColumn,
                           NodeContext nodeCtx) {
                this.referencedRefsToGeneratedColumn = referencedRefsToGeneratedColumn;
                this.nodeCtx = nodeCtx;
            }
        }

        ComparisonReplaceVisitor() {
            super();
        }

        Symbol addComparisons(Symbol symbol,
                              List<GeneratedReference> generatedCols,
                              List<Reference> expansionCandidates,
                              NodeContext nodeCtx) {
            HashMap<Reference, ArrayList<GeneratedReference>> referencedSingleReferences =
                extractGeneratedReferences(generatedCols, expansionCandidates);
            if (referencedSingleReferences.isEmpty()) {
                return symbol;
            } else {
                Context ctx = new Context(referencedSingleReferences, nodeCtx);
                return symbol.accept(this, ctx);
            }
        }

        @Override
        public Symbol visitFunction(Function function, Context context) {
            if (Operators.COMPARISON_OPERATORS.contains(function.name())) {
                Reference reference = null;
                Symbol otherSide = null;
                for (int i = 0; i < function.arguments().size(); i++) {
                    Symbol arg = function.arguments().get(i);
                    arg = Symbols.unwrapReferenceFromCast(arg);
                    if (arg instanceof Reference ref) {
                        reference = ref;
                    } else {
                        otherSide = arg;
                    }
                }
                if (reference != null
                    && otherSide != null
                    && !SymbolVisitors.any(Symbols.IS_GENERATED_COLUMN, otherSide)) {
                    return addComparison(function, reference, otherSide, context);
                }
            }
            return super.visitFunction(function, context);
        }


        private Symbol addComparison(Function function, Reference reference, Symbol comparedAgainst, Context context) {
            ArrayList<GeneratedReference> genColInfos = context.referencedRefsToGeneratedColumn
                .computeIfAbsent(reference, (k) -> new ArrayList<>());
            List<Function> comparisonsToAdd = new ArrayList<>(genColInfos.size());
            comparisonsToAdd.add(function);
            for (GeneratedReference genColInfo : genColInfos) {
                Function comparison = createAdditionalComparison(
                    function,
                    genColInfo,
                    comparedAgainst,
                    context.nodeCtx
                );
                if (comparison != null) {
                    comparisonsToAdd.add(comparison);
                }
            }
            return AndOperator.join(comparisonsToAdd);
        }

        @Nullable
        private Function createAdditionalComparison(Function function,
                                                    GeneratedReference generatedReference,
                                                    Symbol comparedAgainst,
                                                    NodeContext nodeCtx) {
            if (generatedReference != null &&
                generatedReference.generatedExpression().symbolType().equals(SymbolType.FUNCTION)) {

                Function generatedFunction = (Function) generatedReference.generatedExpression();

                String operatorName = function.name();
                if (!operatorName.equals(EqOperator.NAME)) {
                    if (!generatedFunction.signature().hasFeature(Scalar.Feature.COMPARISON_REPLACEMENT)) {
                        return null;
                    }
                    // rewrite operator
                    if (ROUNDING_FUNCTIONS.contains(generatedFunction.name())) {
                        String replacedOperatorName = ROUNDING_FUNCTION_MAPPING.get(operatorName);
                        if (replacedOperatorName != null) {
                            operatorName = replacedOperatorName;
                        }
                    }
                }

                Symbol wrapped = wrapInGenerationExpression(comparedAgainst, generatedReference);
                var funcImpl = nodeCtx.functions().get(
                    null,
                    operatorName,
                    List.of(generatedReference, wrapped),
                    SearchPath.pathWithPGCatalogAndDoc()
                );
                return new Function(
                    funcImpl.signature(),
                    List.of(generatedReference, wrapped),
                    funcImpl.boundSignature().returnType()
                );
            }
            return null;
        }

        private Symbol wrapInGenerationExpression(Symbol wrapMeLikeItsHot, GeneratedReference generatedReference) {
            ReplaceIfMatch replaceIfMatch = new ReplaceIfMatch(
                wrapMeLikeItsHot,
                ((GeneratedReference) generatedReference).referencedReferences().get(0));

            return RefReplacer.replaceRefs(
                ((GeneratedReference) generatedReference).generatedExpression(),
                replaceIfMatch
            );
        }

        private static HashMap<Reference, ArrayList<GeneratedReference>> extractGeneratedReferences(
            List<GeneratedReference> generatedCols,
            Collection<Reference> partitionCols) {
            HashMap<Reference, ArrayList<GeneratedReference>> map = new HashMap<>();
            for (GeneratedReference generatedColumn : generatedCols) {
                if (generatedColumn.referencedReferences().size() == 1 && partitionCols.contains(generatedColumn)) {
                    map
                        .computeIfAbsent(generatedColumn.referencedReferences().get(0), v -> new ArrayList<>())
                        .add(generatedColumn);

                }
            }
            return map;
        }
    }


    static class ReplaceIfMatch implements java.util.function.Function<Reference, Symbol> {

        private final Symbol replaceWith;
        private final Reference toReplace;

        ReplaceIfMatch(Symbol replaceWith, Reference toReplace) {
            this.replaceWith = replaceWith;
            this.toReplace = toReplace;
        }

        @Override
        public Symbol apply(Reference ref) {
            if (ref.equals(toReplace)) {
                return replaceWith;
            }
            return ref;
        }
    }
}
