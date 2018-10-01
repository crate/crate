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

package io.crate.planner.statement;

import io.crate.analyze.SetLicenseAnalyzedStatement;
import io.crate.analyze.SymbolEvaluator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.license.LicenseMetaData;
import io.crate.license.SetLicenseRequest;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;

import java.util.Locale;
import java.util.function.Function;

public class SetLicensePlan implements Plan {

    private final SetLicenseAnalyzedStatement stmt;

    public SetLicensePlan(final SetLicenseAnalyzedStatement stmt) {
        this.stmt = stmt;
    }

    static <T, R> R cast(Function<T, R> function, T argument, Symbol subject) {
        try {
            return function.apply(argument);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "SET LICENSE - Invalid type for setting '%s'", subject.toString()), e);
        }
    }

    @Override
    public StatementType type() {
        return StatementType.MANAGEMENT;
    }

    @Override
    public void execute(DependencyCarrier executor,
                        PlannerContext plannerContext,
                        RowConsumer consumer,
                        Row params,
                        SubQueryResults subQueryResults) {

        Long expirationDateMs = cast(DataTypes.TIMESTAMP::value,
            SymbolEvaluator.evaluate(plannerContext.functions(), stmt.expirationDateSymbol(), params, subQueryResults),
            stmt.expirationDateSymbol());

        BytesRef issueTo = cast(DataTypes.STRING::value,
            SymbolEvaluator.evaluate(plannerContext.functions(), stmt.issuedToSymbol(), params, subQueryResults),
            stmt.issuedToSymbol());

        BytesRef signature = cast(DataTypes.STRING::value,
            SymbolEvaluator.evaluate(plannerContext.functions(), stmt.signatureSymbol(), params, subQueryResults),
            stmt.signatureSymbol());

        LicenseMetaData metaData = new LicenseMetaData(expirationDateMs,
            issueTo.utf8ToString(), signature.utf8ToString());
        SetLicenseRequest request = new SetLicenseRequest(metaData);
        executor.transportActionProvider().transportSetLicenseAction().execute(request, new
            OneRowActionListener<>(consumer, response -> new Row1(1L)));
    }
}
