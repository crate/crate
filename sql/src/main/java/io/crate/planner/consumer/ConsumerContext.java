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

package io.crate.planner.consumer;


import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.exceptions.ValidationException;
import io.crate.planner.Planner;
import org.elasticsearch.common.Nullable;

public class ConsumerContext {

    private AnalyzedRelation rootRelation;
    private ValidationException validationException;
    private Planner.Context plannerContext;

    public ConsumerContext(AnalyzedRelation rootRelation, Planner.Context plannerContext) {
        this.rootRelation = rootRelation;
        this.plannerContext = plannerContext;
    }

    public void rootRelation(AnalyzedRelation relation) {
        this.rootRelation = relation;
    }

    public AnalyzedRelation rootRelation() {
        return rootRelation;
    }

    public void validationException(ValidationException validationException){
        this.validationException = validationException;
    }

    @Nullable
    public ValidationException validationException(){
        return validationException;
    }

    public Planner.Context plannerContext() {
        return plannerContext;
    }
}
