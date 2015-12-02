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

package io.crate.planner.consumer;

import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.TwoTableJoin;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.exceptions.ValidationException;
import io.crate.sql.tree.QualifiedName;

import java.util.Iterator;
import java.util.Map;

public class ManyTableConsumer implements Consumer {

    private final Visitor visitor;

    public ManyTableConsumer(ConsumingPlanner consumingPlanner) {
        this.visitor = new Visitor(consumingPlanner);
    }

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        private final ConsumingPlanner consumingPlanner;

        public Visitor(ConsumingPlanner consumingPlanner) {
            this.consumingPlanner = consumingPlanner;
        }

        @Override
        public PlannedAnalyzedRelation visitMultiSourceSelect(MultiSourceSelect mss, ConsumerContext context) {
            if (mss.sources().size() > 2) {
                context.validationException(new ValidationException("Joining more than 2 tables is not supported"));
            }
            Iterator<Map.Entry<QualifiedName, MultiSourceSelect.Source>> it = mss.sources().entrySet().iterator();
            Map.Entry<QualifiedName, MultiSourceSelect.Source> leftEntry = it.next();
            Map.Entry<QualifiedName, MultiSourceSelect.Source> rightEntry = it.next();

            QuerySpec newQuerySpec = mss.querySpec();
            TwoTableJoin join = new TwoTableJoin(
                    newQuerySpec,
                    leftEntry.getKey(),
                    leftEntry.getValue(),
                    rightEntry.getKey(),
                    rightEntry.getValue(),
                    mss.remainingOrderBy());

            if (context.isRoot()) {
                return consumingPlanner.plan(join, context);
            }
            return context.plannerContext().planSubRelation(join, context);
        }
    }
}
