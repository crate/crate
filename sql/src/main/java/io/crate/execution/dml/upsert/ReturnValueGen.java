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

package io.crate.execution.dml.upsert;

import io.crate.data.Input;
import io.crate.expression.BaseImplementationSymbolVisitor;
import io.crate.expression.reference.Doc;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;

import java.util.ArrayList;

final class ReturnValueGen {

    private final ReturnEvaluator returnEval;

    ReturnValueGen(TransactionContext txnCtx, Functions functions) {
        this.returnEval = new ReturnEvaluator(functions, txnCtx);
    }

    Object[] generateReturnValues(Doc doc, Symbol[] returnValues) {
        ArrayList<Object> result = new ArrayList<>();
        for (int i = 0; i < returnValues.length; i++) {
            Symbol returnValue = returnValues[i];
            Object value = returnValue.accept(returnEval, doc).value();
            result.add(value);
        }
        return result.toArray();
    }

    private static class ReturnEvaluator extends BaseImplementationSymbolVisitor<Doc> {

        private ReturnEvaluator(Functions functions, TransactionContext txnCtx) {
            super(txnCtx, functions);
        }

        @Override
        public Input<?> visitField(Field field, Doc context) {
            return Literal.of(field.valueType(), context.getSource().get(field.path().name()));
        }
    }
}
