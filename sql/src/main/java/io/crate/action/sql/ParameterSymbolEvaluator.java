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

package io.crate.action.sql;

import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.ParamSymbols;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.data.Row;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import org.elasticsearch.common.Nullable;

public class ParameterSymbolEvaluator {

    private static final ValueExtractor SYMBOL_VALUE_EXTRACTOR = new ValueExtractor();

    /**
     * Normalizes the {@link Symbol} using the bounded {@link Row} params
     * and extracts the value of a {@link Literal}
     */
    public static Object eval(Functions functions, Row params, Symbol symbol, @Nullable TransactionContext transactionContext) {
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions);
        symbol = normalizer.normalize(ParamSymbols.toLiterals(symbol, params), transactionContext);
        return SYMBOL_VALUE_EXTRACTOR.process(symbol, params);
    }

    private static class ValueExtractor extends SymbolVisitor<Row, Object> {

        @Override
        public Object visitLiteral(Literal symbol, Row context) {
            return symbol.value();
        }
    }
}
