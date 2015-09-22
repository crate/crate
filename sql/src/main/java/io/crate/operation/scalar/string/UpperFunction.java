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
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.List;
import java.util.Locale;

public class UpperFunction extends Scalar<BytesRef, Object> {
    public static final String NAME = "upper";

    public static void register(ScalarFunctionModule module) {
        List<DataType> supportedLowerTypes = ImmutableList.<DataType>of(
                DataTypes.STRING);

        for (DataType dataType : supportedLowerTypes) {
            // without locale
            module.register(new UpperFunction(new FunctionInfo(
                    new FunctionIdent(NAME, ImmutableList.of(dataType)),
                    DataTypes.STRING)
            ));
            // with locale
            module.register(new UpperFunction(new FunctionInfo(
                    new FunctionIdent(NAME, ImmutableList.of(dataType, DataTypes.STRING)),
                    DataTypes.STRING)
            ));
        }
    }

    private FunctionInfo info;

    public UpperFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public BytesRef evaluate(Input<Object>... args) {
        if (hasNullInputs(args)) {
            return null;
        }

        BytesRef inputByteRef = BytesRefs.toBytesRef(args[0].value());

        Locale currentLocale = Locale.getDefault();
        if (args.length == 2) {
            String localeString = BytesRefs.toBytesRef(args[1].value()).utf8ToString();
            currentLocale = Locale.forLanguageTag(localeString);
        }

        return new BytesRef(inputByteRef.utf8ToString().toUpperCase(currentLocale));
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        assert symbol != null;
        assert symbol.arguments().size() > 0 && symbol.arguments().size() < 3 : "invalid number of arguments";

        return evaluateIfLiterals(this, symbol);
    }
}
