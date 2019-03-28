/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.scalar.systeminformation;

import com.google.common.collect.ImmutableList;
import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.FunctionFormatSpec;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.Collections;

public class UserFunction extends Scalar<String, Object> implements FunctionFormatSpec {

    public static final String CURRENT_USER_FUNCTION_NAME = "current_user";
    public static final String SESSION_USER_FUNCTION_NAME = "session_user";

    private final String name;
    private final FunctionInfo functionInfo;

    public UserFunction(String name) {
        this.name = name;
        this.functionInfo = new FunctionInfo(
            new FunctionIdent(name, ImmutableList.of()),
            DataTypes.STRING,
            FunctionInfo.Type.SCALAR,
            Collections.emptySet());
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    @Override
    public String evaluate(TransactionContext txnCtx, Input<Object>... args) {
        assert args.length == 0 : "number of args must be 0";
        return txnCtx.sessionSettings().userName();
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, @Nullable TransactionContext txnCtx) {
        if (txnCtx == null) {
            return Literal.NULL;
        }
        return Literal.of(txnCtx.sessionSettings().userName());
    }

    @Override
    public String beforeArgs(Function function) {
        return name;
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
