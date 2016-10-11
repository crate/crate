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

package io.crate.executor.transport;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.analyze.symbol.*;
import io.crate.concurrent.CompletionListenable;
import io.crate.core.collections.Row;
import io.crate.operation.projectors.*;

import java.util.List;
import java.util.ListIterator;
import java.util.Set;

class SingleRowSubselectReceiver implements RowReceiver, CompletionListenable {

    private final SettableFuture<Void> completionFuture = SettableFuture.create();
    private final int index;
    private final List<Symbol> subSelectParents;
    private final Visitor visitor = new Visitor();
    private Object value;

    SingleRowSubselectReceiver(int index, List<Symbol> subSelectParents) {
        this.index = index;
        this.subSelectParents = subSelectParents;
    }

    @Override
    public Result setNextRow(Row row) {
        if (this.value == null) {
            this.value = row.get(0);
        } else {
            throw new UnsupportedOperationException("Subquery returned more than 1 row");
        }
        return Result.CONTINUE;
    }

    @Override
    public void pauseProcessed(ResumeHandle resumeable) {
    }

    @Override
    public void finish(RepeatHandle repeatable) {
        replaceSelectSymbols();
        completionFuture.set(null);
    }

    private void replaceSelectSymbols() {
        for (Symbol subSelectParent : subSelectParents) {
            visitor.process(subSelectParent, null);
        }
    }

    @Override
    public void fail(Throwable throwable) {
        completionFuture.setException(throwable);
    }

    @Override
    public void kill(Throwable throwable) {
        completionFuture.setException(throwable);
    }

    @Override
    public void prepare() {
    }

    @Override
    public Set<Requirement> requirements() {
        return Requirements.NO_REQUIREMENTS;
    }

    @Override
    public ListenableFuture<?> completionFuture() {
        return completionFuture;
    }

    private class Visitor extends SymbolVisitor<Void, Symbol> {
        @Override
        protected Symbol visitSymbol(Symbol symbol, Void context) {
            return symbol;
        }

        @Override
        public Symbol visitFunction(Function function, Void context) {
            ListIterator<Symbol> it = function.arguments().listIterator();
            while (it.hasNext()) {
                Symbol arg = it.next();
                it.set(process(arg, context));
            }
            return function;
        }

        @Override
        public Symbol visitSelectSymbol(SelectSymbol selectSymbol, Void context) {
            if (selectSymbol.index() == index) {
                return Literal.of(selectSymbol.valueType(), value);
            }
            return selectSymbol;
        }
    }
}
