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

package io.crate.execution.expression.scalar.systeminformation;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.FunctionFormatSpec;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.execution.expression.scalar.ScalarFunctionModule;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nullable;
import java.util.Collections;


public class CurrentSchemaFunction extends Scalar<BytesRef, Object> implements FunctionFormatSpec {

    public static final String NAME = "current_schema";

    public static final FunctionInfo INFO = new FunctionInfo(
        new FunctionIdent(NAME, ImmutableList.of()),
        DataTypes.STRING,
        FunctionInfo.Type.SCALAR,
        Collections.emptySet());

    public static void register(ScalarFunctionModule scalarFunctionModule) {
        scalarFunctionModule.register(new CurrentSchemaFunction());
    }

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    @Override
    public BytesRef evaluate(Input<Object>... args) {
        assert args.length == 0 : "number of args must be 0";
        throw new UnsupportedOperationException("Cannot evaluate CURRENT_SCHEMA function.");
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, @Nullable TransactionContext transactionContext) {
        if (transactionContext == null) {
            return Literal.NULL;
        }
        assert transactionContext.sessionContext() != null : "CURRENT_SCHEMA requires a session context";
        return Literal.of(transactionContext.sessionContext().defaultSchema());
    }

    @Override
    public String beforeArgs(Function function) {
        return NAME;
    }

    @Override
    public String afterArgs(Function function) {
        return "";
    }

    @Override
    public boolean formatArgs(Function function) {
        return false;
    }
}
