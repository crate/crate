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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.symbol.*;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.operator.EqOperator;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Map;

@Singleton
public class GeneratedColumnComparisonReplacer {

    private static final Predicate<Symbol> IS_GENERATED_COLUMN = new Predicate<Symbol>() {
        @Override
        public boolean apply(@Nullable Symbol input) {
            return input instanceof Reference && ((Reference)input).info() instanceof GeneratedReferenceInfo;
        }
    };


    private final ComparisonReplaceVisitor comparisonReplaceVisitor;

    public GeneratedColumnComparisonReplacer() {
        this.comparisonReplaceVisitor = new ComparisonReplaceVisitor();
    }

    public Symbol replaceIfPossible(Symbol symbol, DocTableInfo tableInfo) {
        return comparisonReplaceVisitor.addComparisons(symbol, tableInfo);
    }

    private static class ComparisonReplaceVisitor extends ReplacingSymbolVisitor<ComparisonReplaceVisitor.Context> {

        private static ReferenceReplacer REFERENCE_REPLACER = new ReferenceReplacer();

        public static class Context {
            private final Map<ReferenceInfo, GeneratedReferenceInfo> referencedRefsToGeneratedColumn;

            public Context(Map<ReferenceInfo, GeneratedReferenceInfo> referencedRefsToGeneratedColumn) {
                this.referencedRefsToGeneratedColumn = referencedRefsToGeneratedColumn;
            }
        }

        public ComparisonReplaceVisitor() {
            super(false);
        }

        public Symbol addComparisons(Symbol symbol, DocTableInfo tableInfo) {
            Map<ReferenceInfo, GeneratedReferenceInfo> referencedSingleReferences = extractGeneratedReferences(tableInfo);
            if (referencedSingleReferences.isEmpty()) {
                return symbol;
            } else {
                Context ctx = new Context(referencedSingleReferences);
                return process(symbol, ctx);
            }
        }

        @Override
        public Symbol visitFunction(Function function, Context context) {
            if (function.info().ident().name().equals(EqOperator.NAME)) {
                Reference reference = null;
                Symbol otherSide = null;
                for (int i = 0; i < function.arguments().size(); i++) {
                    Symbol arg = function.arguments().get(i);
                    if (arg instanceof Reference) {
                        reference = (Reference)arg;
                    } else {
                        otherSide = arg;
                    }
                }
                if (reference != null
                    && otherSide != null
                    && !SymbolVisitors.any(IS_GENERATED_COLUMN, otherSide)) {
                    return addComparison(function, reference, otherSide, context);
                }
            }
            return super.visitFunction(function, context);
        }


        private Function addComparison(Function function, Reference reference, Symbol comparedAgainst, Context context) {
            GeneratedReferenceInfo genColInfo = context.referencedRefsToGeneratedColumn.get(reference.info());
            if (genColInfo != null) {
                Reference genColReference = new Reference(genColInfo);
                Symbol wrapped = wrapInGenerationExpression(comparedAgainst, genColReference);
                FunctionInfo comparisonFunctionInfo = new FunctionInfo(new FunctionIdent(EqOperator.NAME,
                                                      Arrays.asList(genColReference.info().type(), wrapped.valueType())), DataTypes.BOOLEAN);
                Function comparisonFunction = new Function(comparisonFunctionInfo, Arrays.asList(genColReference, wrapped));
                return new Function(AndOperator.INFO, Arrays.<Symbol>asList(function, comparisonFunction));

            }
            return function;
        }

        private Symbol wrapInGenerationExpression(Symbol wrapMeLikeItsHot, Reference generatedReference) {
            ReferenceReplacer.Context ctx = new ReferenceReplacer.Context(wrapMeLikeItsHot,
                    ((GeneratedReferenceInfo)generatedReference.info()).referencedReferenceInfos().get(0));
            return REFERENCE_REPLACER.process(((GeneratedReferenceInfo) generatedReference.info()).generatedExpression(), ctx);
        }

        private Map<ReferenceInfo, GeneratedReferenceInfo> extractGeneratedReferences(DocTableInfo tableInfo) {
            ImmutableMap.Builder<ReferenceInfo, GeneratedReferenceInfo> builder = ImmutableMap.builder();
            for (GeneratedReferenceInfo referenceInfo : tableInfo.generatedColumns()) {
                if (referenceInfo.referencedReferenceInfos().size() == 1) {
                    builder.put(referenceInfo.referencedReferenceInfos().get(0), referenceInfo);
                }
            }
            return builder.build();
        }
    }

    private static class ReferenceReplacer extends ReplacingSymbolVisitor<ReferenceReplacer.Context> {

        public static class Context {

            private final Symbol replaceWith;
            private final ReferenceInfo toReplace;

            public Context(Symbol replaceWith, ReferenceInfo toReplace) {
                this.replaceWith = replaceWith;
                this.toReplace = toReplace;
            }
        }

        public ReferenceReplacer() {
            super(false);
        }

        @Override
        public Symbol visitReference(Reference symbol, Context context) {
            if (symbol.info().equals(context.toReplace)) {
                return context.replaceWith;
            }
            return symbol;
        }
    }
}
