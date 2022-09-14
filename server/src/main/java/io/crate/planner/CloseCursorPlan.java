/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner;

import static io.crate.data.SentinelRow.SENTINEL;
import io.crate.analyze.AnalyzedCloseCursor;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.planner.operators.SubQueryResults;
import io.crate.protocols.postgres.Cursor;
import io.crate.protocols.postgres.Portal;
import io.crate.protocols.postgres.Portals;

import java.util.Iterator;

public class CloseCursorPlan implements Plan {

    private final AnalyzedCloseCursor analyzedCloseCursor;

    public CloseCursorPlan(AnalyzedCloseCursor analyzedCloseCursor) {
        this.analyzedCloseCursor = analyzedCloseCursor;
    }

    @Override
    public StatementType type() {
        // similar to NoopPlan
        return StatementType.UNDEFINED;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) throws Exception {
        String cursorName = analyzedCloseCursor.cursorName();
        Portals portals = analyzedCloseCursor.portals();

        // close all
        if (cursorName == null) {
            Iterator<Portal> it = portals.values().iterator();
            while (it.hasNext()) {
                Portal p = it.next();
                if (p instanceof Cursor) {
                    p.closeActiveConsumer();
                    it.remove();
                }
            }
        } else {
            Portal p = portals.remove(cursorName);
            p.closeActiveConsumer();
        }

        // similar to NoopPlan
        consumer.accept(InMemoryBatchIterator.empty(SENTINEL), null);
    }
}
