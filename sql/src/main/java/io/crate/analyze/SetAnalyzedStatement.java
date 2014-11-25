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

package io.crate.analyze;

import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.Set;

public class SetAnalyzedStatement extends AnalyzedStatement {
    private Settings settings;
    private Set<String> settingsToRemove;
    private boolean persistent = false;
    private boolean isReset = false;

    protected SetAnalyzedStatement(ParameterContext parameterContext) {
        super(parameterContext);
    }

    public Settings settings() {
        return settings;
    }

    public void settings(Settings settings) {
        this.settings = settings;
    }

    @Nullable
    public Set<String> settingsToRemove() {
        return settingsToRemove;
    }

    public void settingsToRemove(Set<String> settingsToRemove) {
        this.settingsToRemove = settingsToRemove;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public boolean isReset() {
        return isReset;
    }

    public void isReset(boolean isReset) {
        this.isReset = isReset;
    }

    public void persistent(boolean persistent) {
        this.persistent = persistent;
    }

    @Override
    public boolean hasNoResult() {
        if (settings != null) {
            return settings.getAsMap().isEmpty();
        }
        if (settingsToRemove != null) {
            return settingsToRemove.size() == 0;
        }
        return true;
    }

    @Override
    public void normalize() {

    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitSetStatement(this, context);
    }
}
