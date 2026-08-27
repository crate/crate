/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.scalar;

import java.util.BitSet;

import io.crate.data.Input;
import io.crate.expression.operator.Operator;
import io.crate.expression.scalar.object.ObjectMergeFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.TypeVariableConstraint;
import io.crate.sql.tree.BitString;
import io.crate.types.BitStringType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public abstract class ConcatFunction extends Scalar<String, String> {

    public static final String NAME = "concat";
    public static final String OPERATOR_NAME = Operator.PREFIX + "||";

    public static void register(Functions.Builder module) {
        module.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.NOTNULL)
                .build(),
            StringConcatFunction::new
        );

        module.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.NOTNULL)
                .setVariableArity(true)
                .build(),
            GenericConcatFunction::new
        );

        // concat(array[], array[]) -> same as `array_cat(...)`
        module.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(TypeSignature.ARRAY_E,
                    TypeSignature.ARRAY_E)
                .returnType(TypeSignature.ARRAY_E)
                .features(Feature.DETERMINISTIC, Feature.NOTNULL)
                .typeVariableConstraints(TypeVariableConstraint.E)
                .build(),
            ArrayCatFunction::new
        );
        module.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.UNTYPED_OBJECT.getTypeSignature(),
                    DataTypes.UNTYPED_OBJECT.getTypeSignature())
                .returnType(DataTypes.UNTYPED_OBJECT.getTypeSignature())
                .features(Feature.DETERMINISTIC)
                .bindActualTypes()
                .build(),
            (signature, boundSignature) -> {
                DataType<?> returnType = ObjectMergeFunction.merge(
                    boundSignature.argTypes().get(0),
                    boundSignature.argTypes().get(1)
                );
                return new ObjectMergeFunction(signature, boundSignature.withReturnType(returnType));
            }
        );


        // Operator versions of concat, the default(string) version differs
        // as it will return null if any of the arguments is null
        module.add(
            Signature.builder(OPERATOR_NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            (signature, boundSignature) -> new StringConcatFunction(signature, boundSignature, true)
        );
        module.add(
            Signature.builder(OPERATOR_NAME, FunctionType.SCALAR)
                .argumentTypes(TypeSignature.ARRAY_E,
                    TypeSignature.ARRAY_E)
                .returnType(TypeSignature.ARRAY_E)
                .features(Feature.DETERMINISTIC)
                .typeVariableConstraints(TypeVariableConstraint.E)
                .build(),
            ArrayCatFunction::new
        );
        module.add(
            Signature.builder(OPERATOR_NAME, FunctionType.SCALAR)
                .argumentTypes(
                    TypeSignature.ARRAY_E,
                    TypeSignature.E
                )
                .returnType(TypeSignature.ARRAY_E)
                .typeVariableConstraints(TypeVariableConstraint.E)
                .features(Feature.DETERMINISTIC)
                .build(),
            (signature, boundSignature) -> new ArrayAppendFunction(signature, boundSignature, true)
        );
        module.add(
            Signature.builder(OPERATOR_NAME, FunctionType.SCALAR)
                .argumentTypes(
                    TypeSignature.E,
                    TypeSignature.ARRAY_E
                )
                .returnType(TypeSignature.ARRAY_E)
                .typeVariableConstraints(TypeVariableConstraint.E)
                .features(Feature.DETERMINISTIC)
                .build(),
            (signature, boundSignature) -> new ArrayPrependFunction(signature, boundSignature, true)
        );
        module.add(
            Signature.builder(OPERATOR_NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.UNTYPED_OBJECT.getTypeSignature(),
                    DataTypes.UNTYPED_OBJECT.getTypeSignature())
                .returnType(DataTypes.UNTYPED_OBJECT.getTypeSignature())
                .features(Feature.DETERMINISTIC)
                .bindActualTypes()
                .build(),
            (signature, boundSignature) -> {
                DataType<?> returnType = ObjectMergeFunction.merge(
                    boundSignature.argTypes().get(0),
                    boundSignature.argTypes().get(1)
                );
                return new ObjectMergeFunction(signature, boundSignature.withReturnType(returnType));
            }
        );
        module.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(BitStringType.INSTANCE_ONE.getTypeSignature(),
                    BitStringType.INSTANCE_ONE.getTypeSignature())
                .returnType(BitStringType.INSTANCE_ONE.getTypeSignature())
                .features(Feature.DETERMINISTIC)
                .bindActualTypes()
                .build(),
            (signature, boundSignature) -> createBitStringConcatFunction(signature, boundSignature, false)
        );
        module.add(
            Signature.builder(OPERATOR_NAME, FunctionType.SCALAR)
                .argumentTypes(BitStringType.INSTANCE_ONE.getTypeSignature(),
                    BitStringType.INSTANCE_ONE.getTypeSignature())
                .returnType(BitStringType.INSTANCE_ONE.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .bindActualTypes()
                .build(),
            (signature, boundSignature) -> createBitStringConcatFunction(signature, boundSignature, true)
        );
    }

    private static BitStringConcatFunction createBitStringConcatFunction(
        Signature signature, BoundSignature boundSignature, boolean strictNull) {

        DataType<?> arg0 = boundSignature.argTypes().get(0);
        DataType<?> arg1 = boundSignature.argTypes().get(1);

        int len1 = arg0 instanceof BitStringType bs ? bs.length() : 0;
        int len2 = arg1 instanceof BitStringType bs ? bs.length() : 0;

        DataType<?> returnType = new BitStringType(len1 + len2);
        return new BitStringConcatFunction(signature, boundSignature.withReturnType(returnType), strictNull);
    }

    ConcatFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Symbol normalizeSymbol(Function function, TransactionContext txnCtx, NodeContext nodeCtx) {
        if (anyNonLiterals(function.arguments())) {
            return function;
        }
        Input[] inputs = new Input[function.arguments().size()];
        for (int i = 0; i < function.arguments().size(); i++) {
            inputs[i] = ((Input) function.arguments().get(i));
        }
        //noinspection unchecked
        return Literal.ofUnchecked(boundSignature.returnType(), evaluate(txnCtx, nodeCtx, inputs));
    }

    static class StringConcatFunction extends ConcatFunction {

        private final boolean calledByOperator;

        StringConcatFunction(Signature signature, BoundSignature boundSignature) {
            super(signature, boundSignature);
            calledByOperator = false;
        }

        StringConcatFunction(Signature signature,
                             BoundSignature boundSignature,
                             boolean calledByOperator) {
            super(signature, boundSignature);
            this.calledByOperator = calledByOperator;
        }

        @SuppressWarnings("rawtypes")
        @Override
        public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
            String firstArg = (String) args[0].value();
            String secondArg = (String) args[1].value();

            if (calledByOperator && (firstArg == null || secondArg == null)) {
                return null;
            }

            if (firstArg == null) {
                if (secondArg == null) {
                    return "";
                }
                return secondArg;
            }
            if (secondArg == null) {
                return firstArg;
            }
            return firstArg + secondArg;
        }


    }

    private static class GenericConcatFunction extends ConcatFunction {

        public GenericConcatFunction(Signature signature, BoundSignature boundSignature) {
            super(signature, boundSignature);
        }

        @Override
        public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>[] args) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < args.length; i++) {
                String value = args[i].value();
                if (value != null) {
                    sb.append(value);
                }
            }
            return sb.toString();
        }
    }

    static class BitStringConcatFunction extends Scalar<BitString, BitString> {

        private final boolean strictNull;

        /**
         * @param strictNull return null on any null input. If false returns the non-null arg if one is null
         */
        BitStringConcatFunction(Signature signature, BoundSignature boundSignature, boolean strictNull) {
            super(signature, boundSignature);
            this.strictNull = strictNull;
        }

        @Override
        public BitString evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<BitString>[] args) {
            BitString firstArg = args[0].value();
            BitString secondArg = args[1].value();

            if (strictNull && (firstArg == null || secondArg == null)) {
                return null;
            }
            if (firstArg == null) {
                return secondArg;
            }
            if (secondArg == null) {
                return firstArg;
            }

            int newLength = firstArg.length() + secondArg.length();
            BitSet newBitSet = new BitSet(newLength);

            BitSet firstBits = firstArg.bitSet();
            for (int i = 0; i < firstArg.length(); i++) {
                if (firstBits.get(i)) {
                    newBitSet.set(i);
                }
            }

            BitSet secondBits = secondArg.bitSet();
            for (int i = 0; i < secondArg.length(); i++) {
                if (secondBits.get(i)) {
                    newBitSet.set(firstArg.length() + i);
                }
            }

            return new BitString(newBitSet, newLength);
        }
    }
}
