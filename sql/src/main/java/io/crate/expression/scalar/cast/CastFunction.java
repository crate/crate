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

package io.crate.expression.scalar.cast;

import io.crate.data.Input;
import io.crate.exceptions.ConversionException;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.function.BiFunction;

import static io.crate.expression.scalar.cast.CastFunctionResolver.CAST_SIGNATURES;
import static io.crate.expression.scalar.cast.CastFunctionResolver.TRY_CAST_PREFIX;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

public class CastFunction extends Scalar<Object, Object> {

    public static void register(ScalarFunctionModule module) {
        // We still maintain the cast function to type mapping to stay
        // bwc by keeping the old `to_<type>` and `try_<type>` function signatures.
        //
        // We can drop the per type cast function already in 4.2. This change
        // would require handling the metadata for the places where the old
        // signature of the cast function were used, e.g. generated columns.
        for (Map.Entry<String, DataType> function : CAST_SIGNATURES.entrySet()) {
            module.register(
                Signature.builder()
                    .name(new FunctionName(null, function.getKey()))
                    .kind(FunctionInfo.Type.SCALAR)
                    .typeVariableConstraints(typeVariable("E"), typeVariable("V"))
                    .argumentTypes(parseTypeSignature("E"), parseTypeSignature("V"))
                    .returnType(function.getValue().getTypeSignature())
                    .build(),
                (signature, args) -> {
                    DataType<?> sourceType = args.get(0);
                    DataType<?> targetType = args.get(1);
                    if (!sourceType.isConvertableTo(targetType)) {
                        throw new ConversionException(sourceType, targetType);
                    }
                    return new CastFunction(
                        new FunctionInfo(new FunctionIdent(function.getKey(), args), targetType),
                        signature,
                        (argument, returnType) -> {
                            throw new ConversionException(argument, returnType);
                        },
                        (argument, returnType) -> {
                            throw new ConversionException(argument, returnType);
                        }
                    );
                }
            );
            // for internal cast functions invocations, e.g. to_bigint(<any type>) -> bigint
            module.register(
                Signature.builder()
                    .name(new FunctionName(null, function.getKey()))
                    .kind(FunctionInfo.Type.SCALAR)
                    .typeVariableConstraints(typeVariable("E"))
                    .argumentTypes(parseTypeSignature("E"))
                    .returnType(function.getValue().getTypeSignature())
                    .forbidCoercion()
                    .build(),
                (signature, args) -> {
                    DataType<?> sourceType = args.get(0);
                    DataType<?> targetType = function.getValue();
                    if (!sourceType.isConvertableTo(targetType)) {
                        throw new ConversionException(sourceType, targetType);
                    }
                    return new CastFunction(
                        new FunctionInfo(new FunctionIdent(function.getKey(), args), targetType),
                        signature,
                        (argument, returnType) -> {
                            throw new ConversionException(argument, returnType);
                        },
                        (argument, returnType) -> {
                            throw new ConversionException(argument, returnType);
                        }
                    );
                }
            );

            var tryCastName = TRY_CAST_PREFIX + function.getKey();
            module.register(
                Signature.builder()
                    .name(new FunctionName(null, tryCastName))
                    .kind(FunctionInfo.Type.SCALAR)
                    .typeVariableConstraints(typeVariable("E"), typeVariable("V"))
                    .argumentTypes(parseTypeSignature("E"), parseTypeSignature("V"))
                    .returnType(function.getValue().getTypeSignature())
                    .build(),
                (signature, args) -> new CastFunction(
                    new FunctionInfo(new FunctionIdent(tryCastName, args), args.get(1)),
                    signature,
                    (argument, returnType) -> Literal.NULL,
                    (argument, returnType) -> null
                )
            );
            // for internal cast functions invocations
            module.register(
                Signature.builder()
                    .name(new FunctionName(null, tryCastName))
                    .kind(FunctionInfo.Type.SCALAR)
                    .typeVariableConstraints(typeVariable("E"))
                    .argumentTypes(parseTypeSignature("E"))
                    .returnType(function.getValue().getTypeSignature())
                    .build(),
                (signature, args) -> new CastFunction(
                    new FunctionInfo(new FunctionIdent(tryCastName, args), function.getValue()),
                    signature,
                    (argument, returnType) -> Literal.NULL,
                    (argument, returnType) -> null
                )
            );
        }
    }

    public static final String TRY_CAST_SQL_NAME = "try_cast";
    public static final String CAST_SQL_NAME = "cast";

    private final DataType<?> returnType;
    private final FunctionInfo info;
    private final Signature signature;
    private final BiFunction<Symbol, DataType<?>, Symbol> onNormalizeException;
    private final BiFunction<Object, DataType<?>, Object> onEvaluateException;

    private CastFunction(FunctionInfo info,
                         Signature signature,
                         BiFunction<Symbol, DataType<?>, Symbol> onNormalizeException,
                         BiFunction<Object, DataType<?>, Object> onEvaluateException) {
        this.info = info;
        this.signature = signature;
        this.returnType = info.returnType();
        this.onNormalizeException = onNormalizeException;
        this.onEvaluateException = onEvaluateException;
    }

    @Override
    public Object evaluate(TransactionContext txnCtx, Input<Object>[] args) {
        Object value = args[0].value();
        try {
            return returnType.value(value);
        } catch (ClassCastException | IllegalArgumentException e) {
            return onEvaluateException.apply(value, returnType);
        }
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx) {
        Symbol argument = symbol.arguments().get(0);
        if (argument instanceof Input) {
            Object value = ((Input<?>) argument).value();
            try {
                return Literal.ofUnchecked(returnType, returnType.value(value));
            } catch (ClassCastException | IllegalArgumentException e) {
                return onNormalizeException.apply(argument, returnType);
            }
        }
        return symbol;
    }
}
