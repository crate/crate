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
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.FunctionCopyVisitor;
import io.crate.analyze.symbol.RefReplacer;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolType;
import io.crate.analyze.symbol.SymbolVisitors;
import io.crate.analyze.symbol.Symbols;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.GtOperator;
import io.crate.operation.operator.GteOperator;
import io.crate.operation.operator.LtOperator;
import io.crate.operation.operator.LteOperator;
import io.crate.operation.operator.Operators;
import io.crate.operation.scalar.DateTruncFunction;
import io.crate.operation.scalar.arithmetic.CeilFunction;
import io.crate.operation.scalar.arithmetic.FloorFunction;
import io.crate.operation.scalar.arithmetic.RoundFunction;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
public class GeneratedColumnComparisonReplacer {


    private static final Map<String, String> ROUNDING_FUNCTION_MAPPING = ImmutableMap.of(
        GtOperator.NAME, GteOperator.NAME,
        LtOperator.NAME, LteOperator.NAME
    );

    private static final Set<String> ROUNDING_FUNCTIONS = ImmutableSet.of(
        CeilFunction.NAME,
        FloorFunction.NAME,
        RoundFunction.NAME,
        DateTruncFunction.NAME
    );

    private static final ComparisonReplaceVisitor COMPARISON_REPLACE_VISITOR = new ComparisonReplaceVisitor();

    public Symbol replaceIfPossible(Symbol symbol, DocTableInfo tableInfo) {
        return COMPARISON_REPLACE_VISITOR.addComparisons(symbol, tableInfo);
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

        Symbol addComparisons(Symbol symbol, DocTableInfo tableInfo) {
            Multimap<Reference, GeneratedReference> referencedSingleReferences = extractGeneratedReferences(tableInfo);
            if (referencedSingleReferences.isEmpty()) {
                return symbol;
            } else {
                Context ctx = new Context(referencedSingleReferences);
                return process(symbol, ctx);
            }
        }

        @Override
        public Symbol visitFunction(Function function, Context context) {
            if (Operators.COMPARISON_OPERATORS.contains(function.info().ident().name())) {
                Reference reference = null;
                Symbol otherSide = null;
                for (int i = 0; i < function.arguments().size(); i++) {
                    Symbol arg = function.arguments().get(i);
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
                    if (!generatedFunction.info().features().contains(FunctionInfo.Feature.COMPARISON_REPLACEMENT)) {
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

        private Multimap<Reference, GeneratedReference> extractGeneratedReferences(DocTableInfo tableInfo) {
            Multimap<Reference, GeneratedReference> multiMap = HashMultimap.create();

            for (GeneratedReference generatedColumn : tableInfo.generatedColumns()) {
                if (generatedColumn.referencedReferences().size() == 1 &&
                    tableInfo.partitionedByColumns().contains(generatedColumn)) {
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
