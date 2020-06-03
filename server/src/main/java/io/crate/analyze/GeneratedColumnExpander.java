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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.FunctionCopyVisitor;
import io.crate.expression.symbol.RefReplacer;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.GtOperator;
import io.crate.expression.operator.GteOperator;
import io.crate.expression.operator.LtOperator;
import io.crate.expression.operator.LteOperator;
import io.crate.expression.operator.Operators;
import io.crate.expression.scalar.DateTruncFunction;
import io.crate.expression.scalar.arithmetic.CeilFunction;
import io.crate.expression.scalar.arithmetic.FloorFunction;
import io.crate.expression.scalar.arithmetic.RoundFunction;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class GeneratedColumnExpander {

    private static final Map<String, String> ROUNDING_FUNCTION_MAPPING = ImmutableMap.of(
        GtOperator.NAME, GteOperator.NAME,
        LtOperator.NAME, LteOperator.NAME
    );

    private static final Set<String> ROUNDING_FUNCTIONS = ImmutableSet.of(
        CeilFunction.CEIL,
        FloorFunction.NAME,
        RoundFunction.NAME,
        DateTruncFunction.NAME
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
    public static Symbol maybeExpand(Symbol symbol, List<GeneratedReference> generatedCols, List<Reference> expansionCandidates) {
        return COMPARISON_REPLACE_VISITOR.addComparisons(symbol, generatedCols, expansionCandidates);
    }

    private static class ComparisonReplaceVisitor extends FunctionCopyVisitor<ComparisonReplaceVisitor.Context> {

        static class Context {
            private final Multimap<Reference, GeneratedReference> referencedRefsToGeneratedColumn;

            public Context(Multimap<Reference, GeneratedReference> referencedRefsToGeneratedColumn) {
                this.referencedRefsToGeneratedColumn = referencedRefsToGeneratedColumn;
            }
        }

        ComparisonReplaceVisitor() {
            super();
        }

        Symbol addComparisons(Symbol symbol, List<GeneratedReference> generatedCols, List<Reference> expansionCandidates) {
            Multimap<Reference, GeneratedReference> referencedSingleReferences =
                extractGeneratedReferences(generatedCols, expansionCandidates);
            if (referencedSingleReferences.isEmpty()) {
                return symbol;
            } else {
                Context ctx = new Context(referencedSingleReferences);
                return symbol.accept(this, ctx);
            }
        }

        @Override
        public Symbol visitFunction(Function function, Context context) {
            if (Operators.COMPARISON_OPERATORS.contains(function.info().ident().name())) {
                Reference reference = null;
                Symbol otherSide = null;
                for (int i = 0; i < function.arguments().size(); i++) {
                    Symbol arg = function.arguments().get(i);
                    arg = Symbols.unwrapReferenceFromCast(arg);
                    if (arg instanceof Reference) {
                        reference = (Reference) arg;
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
            Collection<GeneratedReference> genColInfos = context.referencedRefsToGeneratedColumn.get(reference);
            List<Function> comparisonsToAdd = new ArrayList<>(genColInfos.size());
            comparisonsToAdd.add(function);
            for (GeneratedReference genColInfo : genColInfos) {
                Function comparison = createAdditionalComparison(function, genColInfo, comparedAgainst);
                if (comparison != null) {
                    comparisonsToAdd.add(comparison);
                }
            }
            return AndOperator.join(comparisonsToAdd);
        }

        @Nullable
        private Function createAdditionalComparison(Function function,
                                                    GeneratedReference generatedReference,
                                                    Symbol comparedAgainst) {
            if (generatedReference != null &&
                generatedReference.generatedExpression().symbolType().equals(SymbolType.FUNCTION)) {

                Function generatedFunction = (Function) generatedReference.generatedExpression();
                String operatorName = function.info().ident().name();
                if (!operatorName.equals(EqOperator.NAME)) {
                    if (!generatedFunction.info().hasFeature(FunctionInfo.Feature.COMPARISON_REPLACEMENT)) {
                        return null;
                    }
                    // rewrite operator
                    if (ROUNDING_FUNCTIONS.contains(generatedFunction.info().ident().name())) {
                        String replacedOperatorName = ROUNDING_FUNCTION_MAPPING.get(operatorName);
                        if (replacedOperatorName != null) {
                            operatorName = replacedOperatorName;
                        }
                    }
                }

                Symbol wrapped = wrapInGenerationExpression(comparedAgainst, generatedReference);
                FunctionInfo comparisonFunctionInfo = new FunctionInfo(new FunctionIdent(operatorName,
                    Arrays.asList(generatedReference.valueType(), wrapped.valueType())), DataTypes.BOOLEAN);
                return new Function(comparisonFunctionInfo, Arrays.asList(generatedReference, wrapped));

            }
            return null;
        }

        private Symbol wrapInGenerationExpression(Symbol wrapMeLikeItsHot, Reference generatedReference) {
            ReplaceIfMatch replaceIfMatch = new ReplaceIfMatch(
                wrapMeLikeItsHot,
                ((GeneratedReference) generatedReference).referencedReferences().get(0));

            return RefReplacer.replaceRefs(
                ((GeneratedReference) generatedReference).generatedExpression(),
                replaceIfMatch
            );
        }

        private static Multimap<Reference, GeneratedReference> extractGeneratedReferences(List<GeneratedReference> generatedCols,
                                                                                          Collection<Reference> partitionCols) {
            Multimap<Reference, GeneratedReference> multiMap = HashMultimap.create();
            for (GeneratedReference generatedColumn : generatedCols) {
                if (generatedColumn.referencedReferences().size() == 1 && partitionCols.contains(generatedColumn)) {
                    multiMap.put(generatedColumn.referencedReferences().get(0), generatedColumn);
                }
            }
            return multiMap;
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
