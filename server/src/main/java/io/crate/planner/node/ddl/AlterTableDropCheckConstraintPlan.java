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

package io.crate.planner.node.ddl;

import com.google.common.annotations.VisibleForTesting;
import io.crate.analyze.AnalyzedAlterTableDropCheckConstraint;
import io.crate.analyze.AnalyzedTableElements;
import io.crate.analyze.BoundAddColumn;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import org.elasticsearch.common.settings.Settings;

public class AlterTableDropCheckConstraintPlan implements Plan {

    private final AnalyzedAlterTableDropCheckConstraint dropCheckConstraint;

    public AlterTableDropCheckConstraintPlan(AnalyzedAlterTableDropCheckConstraint dropCheckConstraint) {
        this.dropCheckConstraint = dropCheckConstraint;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {
        dependencies.alterTableOperation()
            .executeAlterTableAddColumn(bind(dropCheckConstraint))
            .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));
    }

    @VisibleForTesting
    public static BoundAddColumn bind(AnalyzedAlterTableDropCheckConstraint dropCheckConstraint) {
        DocTableInfo tableInfo = dropCheckConstraint.tableInfo();
        AnalyzedTableElements<Object> tableElementsBound = new AnalyzedTableElements<>();
        AlterTableAddColumnPlan.addExistingPrimaryKeys(tableInfo, tableElementsBound);
        tableInfo.checkConstraints()
            .stream()
            .filter(c -> !dropCheckConstraint.name().equals(c.name()))
            .forEach(c -> tableElementsBound.addCheckConstraint(tableInfo.ident(), c));
        return new BoundAddColumn(
            tableInfo,
            tableElementsBound,
            Settings.builder().build(),
            AnalyzedTableElements.finalizeAndValidate(
                tableInfo.ident(),
                new AnalyzedTableElements<>(),
                tableElementsBound
            ),
            false,
            false
        );
    }
}
