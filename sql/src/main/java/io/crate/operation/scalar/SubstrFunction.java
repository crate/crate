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

package io.crate.operation.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
import io.crate.sql.tree.StringLiteral;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;

import java.util.List;

public class SubstrFunction implements Scalar<BytesRef, Object>, DynamicFunctionResolver {

    public static final String NAME = "substr";
    private FunctionInfo info;

    public SubstrFunction() {}
    public SubstrFunction(FunctionInfo info) {
        this.info = info;
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new SubstrFunction());
    }

    private static FunctionInfo createInfo(List<DataType> types) {
        return new FunctionInfo(new FunctionIdent(NAME, types), DataTypes.STRING);
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public BytesRef evaluate(Input<Object>... args) {
        assert (args.length >= 2 && args.length <= 3);
        assert args[0].value() != null;

        String inputStr;
        if (args[0].value() instanceof BytesRef) {
            inputStr = ((BytesRef) args[0].value()).utf8ToString();
        } else {
            inputStr = (String) args[0].value();
        }

        int startPos = Math.max(0, ((Number) args[1].value()).intValue() - 1);
        if (startPos > inputStr.length() - 1) {
            return new BytesRef();
        }

        int endPos = inputStr.length();

        if (args.length == 3) {
            int len = ((Number) args[2].value()).intValue();
            if (startPos + len < endPos) {
                endPos = startPos + len;
            }
        }

        return new BytesRef(inputStr.substring(startPos, endPos));
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        int size = symbol.arguments().size();
        assert (size >= 2 && size <= 3);

        if (symbol.arguments().get(0).symbolType() != SymbolType.LITERAL ||
                symbol.arguments().get(1).symbolType() != SymbolType.LITERAL ||
                (size == 3 && symbol.arguments().get(2).symbolType() != SymbolType.LITERAL)) {
            return symbol;
        }

        String inputStr = ((BytesRef)((Literal) symbol.arguments().get(0)).value()).utf8ToString();

        int startPos = Math.max(0, ((Number)((Literal) symbol.arguments().get(1)).value()).intValue() - 1);
        if (startPos > inputStr.length() - 1) {
            return Literal.EMPTY_STRING;
        }

        int endPos = inputStr.length();

        if (size == 3) {
            int len = ((Number)((Literal) symbol.arguments().get(2)).value()).intValue();
            endPos = Math.min(startPos + len, endPos);
        }
        return Literal.newLiteral(inputStr.substring(startPos, endPos));
    }

    @Override
    public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
        Preconditions.checkArgument(dataTypes.size() > 1 && dataTypes.size() < 4 && dataTypes.get(0) == DataTypes.STRING);
        return new SubstrFunction(createInfo(dataTypes));
    }

}
