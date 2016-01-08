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

package io.crate.operation.scalar.string;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.SymbolType;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.List;
import java.util.Locale;

public class LowerFunction extends Scalar<BytesRef, Object> {
    public static final String NAME = "lower";

    private Locale currentLocale;

    public static void register(ScalarFunctionModule module) {
        List<DataType> supportedLowerTypes = ImmutableList.<DataType>of(
                DataTypes.STRING);

        for (DataType dataType : supportedLowerTypes) {
            // without locale
            module.register(new LowerFunction(new FunctionInfo(
                    new FunctionIdent(NAME, ImmutableList.of(dataType)),
                    DataTypes.STRING)
            ));
            // with locale
            module.register(new LowerFunction(new FunctionInfo(
                    new FunctionIdent(NAME, ImmutableList.of(dataType, DataTypes.STRING)),
                    DataTypes.STRING)
            ));
        }
    }

    private FunctionInfo info;

    public LowerFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public BytesRef evaluate(Input<Object>... args) {
        Object stringValue = args[0].value();
        if (stringValue == null) {
            return null;
        }

        BytesRef inputByteRef = BytesRefs.toBytesRef(stringValue);

        if (args.length == 2 && currentLocale == null) {
            Object localeValue = args[1].value();
            // we are dealing with a locale that is passed as a column reference here
            if (localeValue != null) {
                String localeString = BytesRefs.toBytesRef(localeValue).utf8ToString();
                currentLocale = Locale.forLanguageTag(localeString);
            } else {
                currentLocale = Locale.getDefault();
            }
        }

        return new BytesRef(inputByteRef.utf8ToString().toLowerCase(currentLocale));
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Scalar<BytesRef, Object> compile(List<Symbol> arguments) {
        assert arguments.size() > 0 && arguments.size() < 3 : "invalid number of arguments";

        if (arguments.size() < 2) {
            currentLocale = Locale.getDefault();
        } else {
            if (arguments.get(1).symbolType() == SymbolType.LITERAL) {
                Object localeValue = ((Literal) arguments.get(1)).value();
                if (localeValue != null) {
                    currentLocale = Locale.forLanguageTag(BytesRefs.toBytesRef(localeValue).utf8ToString());
                } else {
                    currentLocale = Locale.getDefault();
                }
            }
        }

        return this;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        assert symbol != null;
        assert symbol.arguments().size() > 0 && symbol.arguments().size() < 3 : "invalid number of arguments";

        Symbol arg = symbol.arguments().get(0);
        if (symbol.arguments().size() == 2) {
            Symbol locale = symbol.arguments().get(1);
            return Literal.newLiteral(evaluate((Input) arg, (Input) locale));
        } else {
            return Literal.newLiteral(evaluate((Input) arg));
        }
    }
}
