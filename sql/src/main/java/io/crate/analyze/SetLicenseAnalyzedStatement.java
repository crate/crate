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

package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.expression.symbol.Symbol;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SetLicenseAnalyzedStatement implements AnalyzedStatement {

    static final String EXPIRY_DATE_TOKEN = "expirationdate";
    static final String ISSUED_TO_TOKEN = "issuedto";
    static final String SIGN_TOKEN = "signature";

    static final int LICENSE_TOKEN_NUM = 3;

    static final Set<String> LICENSE_ALLOWED_TOKENS = new HashSet<>(
        Arrays.asList(EXPIRY_DATE_TOKEN, ISSUED_TO_TOKEN, SIGN_TOKEN));

    static final String LICENSE_ALLOWED_TOKENS_AS_STRING = String.join(", ", LICENSE_ALLOWED_TOKENS);

    private static final String MISSING_SETTING_ERROR_TEMPLATE = "Missing setting for SET LICENSE. Please provide the following settings: [%s]";

    private final Symbol expirationDateSymbol;
    private final Symbol issuedToSymbol;
    private final Symbol signatureSymbol;

    SetLicenseAnalyzedStatement(final Symbol expirationDateSymbol, final Symbol issuedToSymbol, final Symbol signatureSymbol) {
        this.expirationDateSymbol = Preconditions.checkNotNull(expirationDateSymbol, MISSING_SETTING_ERROR_TEMPLATE, LICENSE_ALLOWED_TOKENS_AS_STRING);
        this.issuedToSymbol = Preconditions.checkNotNull(issuedToSymbol, MISSING_SETTING_ERROR_TEMPLATE, LICENSE_ALLOWED_TOKENS_AS_STRING);
        this.signatureSymbol = Preconditions.checkNotNull(signatureSymbol, MISSING_SETTING_ERROR_TEMPLATE, LICENSE_ALLOWED_TOKENS_AS_STRING);
    }

    public final Symbol expirationDateSymbol() {
        return expirationDateSymbol;
    }

    public final Symbol issuedToSymbol() {
        return issuedToSymbol;
    }

    public final Symbol signatureSymbol() {
        return signatureSymbol;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitSetLicenseStatement(this, context);
    }

    @Override
    public boolean isWriteOperation() {
        return true;
    }
}
