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

package io.crate.operation.operator.any;

import com.google.common.base.Preconditions;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.operator.LikeOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.planner.symbol.Function;
import io.crate.types.DataType;
import io.crate.types.StringType;
import org.apache.lucene.util.BytesRef;

import java.util.List;
import java.util.regex.Pattern;


public class AnyLikeOperator extends AnyOperator<AnyLikeOperator> {

    public static final String NAME = AnyOperator.OPERATOR_PREFIX + "like";

    public static void register(OperatorModule module) {
        module.registerDynamicOperatorFunction(NAME, new AnyLikeOperator());
    }


    public AnyLikeOperator() {

    }

    public AnyLikeOperator(FunctionInfo info) {
        super(info);
    }

    @Override
    protected Boolean doEvaluate(Object right, Iterable<?> leftIterable) {
        BytesRef rightBytesRef = (BytesRef)right;

        boolean hasNull = false;
        for (Object elem : leftIterable) {
            if (elem == null) {
                hasNull = true;
                continue;
            }
            assert (elem instanceof BytesRef || elem instanceof String);

            String elemValue;
            if (elem instanceof BytesRef) {
                elemValue = ((BytesRef) elem).utf8ToString();
            } else {
                elemValue = (String)elem;
            }
            if (matches(elemValue, rightBytesRef.utf8ToString())) {
                return true;
            }
        }
        return hasNull ? null : false;
    }

    protected boolean matches(String expression, String pattern) {
        return Pattern.matches(
                LikeOperator.patternToRegex(pattern, LikeOperator.DEFAULT_ESCAPE, true),
                expression
        );
    }

    @Override
    protected boolean compare(int comparisonResult) {
        return false;
    }

    @Override
    protected String name() {
        return NAME;
    }

    @Override
    protected AnyLikeOperator newInstance(FunctionInfo info) {
        return new AnyLikeOperator(info);
    }

    @Override
    public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
        Preconditions.checkArgument(dataTypes.get(1).id() == StringType.ID);
        return super.getForTypes(dataTypes);
    }
}
