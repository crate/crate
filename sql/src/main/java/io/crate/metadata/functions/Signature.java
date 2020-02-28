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

package io.crate.metadata.functions;

import io.crate.expression.symbol.FuncArg;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionName;
import io.crate.types.TypeSignature;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toList;

public class Signature {

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private FunctionName name;
        private FunctionInfo.Type kind = FunctionInfo.Type.SCALAR;
        private List<TypeSignature> args = Collections.emptyList();
        private List<TypeVariableConstraint> typeVariableConstraints = new ArrayList<>();
        private TypeSignature returnType;
        private Consumer<List<? extends FuncArg>> validation = a -> {
        };
        private boolean variableArity = false;

        public Builder name(FunctionName name) {
            this.name = name;
            return this;
        }

        public Builder args(TypeSignature... args) {
            this.args = List.of(args);
            return this;
        }

        public Builder returnType(TypeSignature returnType) {
            this.returnType = returnType;
            return this;
        }

        public Builder withVariableConstraint(TypeVariableConstraint typeVariableConstraint) {
            typeVariableConstraints.add(typeVariableConstraint);
            return this;
        }

        public Builder withValidation(Consumer<List<? extends FuncArg>> validation) {
            this.validation = validation;
            return this;
        }

        public Builder withVariableArity() {
            this.variableArity = true;
            return this;
        }

        public Signature build() {
            assert returnType != null : "returnType not set";
            return new Signature(name, kind, typeVariableConstraints, args, returnType, validation, variableArity);
        }

    }

    private final FunctionName name;
    private final FunctionInfo.Type kind;
    private final List<TypeSignature> args;
    private final TypeSignature returnType;
    private final List<TypeVariableConstraint> typeVariableConstraints;
    private final Consumer<List<? extends FuncArg>> validation;
    private final boolean variableArity;

    public Signature(FunctionName name, FunctionInfo.Type kind, List<TypeSignature> args, TypeSignature returnType) {
        this(name, kind, Collections.emptyList(), args, returnType, a -> {
        }, false);
    }

    public Signature(FunctionName name,
                     FunctionInfo.Type kind,
                     List<TypeVariableConstraint> typeVariableConstraints,
                     List<TypeSignature> args,
                     TypeSignature returnType,
                     boolean variableArity) {
        this(name, kind, typeVariableConstraints, args, returnType, a -> {
        }, variableArity);
    }

    public Signature(FunctionName name,
                     FunctionInfo.Type kind,
                     List<TypeVariableConstraint> typeVariableConstraints,
                     List<TypeSignature> args,
                     TypeSignature returnType,
                     Consumer<List<? extends FuncArg>> validation,
                     boolean variableArity) {
        this.name = name;
        this.kind = kind;
        this.args = args;
        this.typeVariableConstraints = typeVariableConstraints;
        this.returnType = returnType;
        this.validation = validation;
        this.variableArity = variableArity;
    }

    public FunctionName getName() {
        return name;
    }

    public FunctionInfo.Type getKind() {
        return kind;
    }

    public List<TypeSignature> getArgumentTypes() {
        return args;
    }

    public TypeSignature getReturnType() {
        return returnType;
    }

    public List<TypeVariableConstraint> getTypeVariableConstraints() {
        return typeVariableConstraints;
    }

    public boolean isVariableArity() {
        return variableArity;
    }

    public boolean match(List<? extends FuncArg> args) {
        if (this.args.size() == 0) {
            return args.size() == 0;
        }
        TypeSignature lastParam = null;
        for (int i = 0; i < args.size(); i++) {
            FuncArg arg = args.get(i);
            if (i < this.args.size()) {
                var param = lastParam = this.args.get(i);
                if (!param.match(arg.valueType())) {
                    return false;
                }
            } else {
                if (lastParam.mode() != TypeSignature.Mode.VARIADIC) {
                    return false;
                }
                if (!lastParam.match(arg.valueType())) {
                    return false;
                }
            }
        }
        validation.accept(args);
        return true;
    }

    @Override
    public String toString() {
        List<String> allConstraints = typeVariableConstraints.stream().map(TypeVariableConstraint::toString)
            .collect(toList());

        return name + (allConstraints.isEmpty() ? "" : "<" + String.join(",", allConstraints) + ">") +
               "(" + String.join(",", args.stream().map(TypeSignature::toString).collect(toList())) + "):" + returnType;
    }
}
