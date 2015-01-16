/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.node.ddl;

import io.crate.planner.node.PlanNodeVisitor;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.Set;

public class ESClusterUpdateSettingsNode extends DDLPlanNode {

    private final Settings persistentSettings;
    private final Settings transientSettings;
    private final Set<String> transientSettingsToRemove;
    private final Set<String> persistentSettingsToRemove;

    public ESClusterUpdateSettingsNode(Settings persistentSettings,
                                       Settings transientSettings) {
        this.persistentSettings = persistentSettings;
        // always override transient settings with persistent ones, so they won't get overridden
        // on cluster settings merge, which prefers the transient ones over the persistent ones
        // which we don't
        this.transientSettings = ImmutableSettings.builder().put(persistentSettings).put(transientSettings).build();
        persistentSettingsToRemove = null;
        transientSettingsToRemove = null;
    }

    public ESClusterUpdateSettingsNode(Settings persistentSettings) {
        this(persistentSettings, persistentSettings); // override stale transient settings too in that case
    }

    public ESClusterUpdateSettingsNode(Set<String> persistentSettingsToRemove, Set<String> transientSettingsToRemove) {
        this.persistentSettingsToRemove = persistentSettingsToRemove;
        this.transientSettingsToRemove = transientSettingsToRemove;
        persistentSettings = ImmutableSettings.EMPTY;
        transientSettings = ImmutableSettings.EMPTY;
    }

    public ESClusterUpdateSettingsNode(Set<String> persistentSettingsToRemove) {
        this(persistentSettingsToRemove, persistentSettingsToRemove);
    }

    public Settings persistentSettings() {
        return persistentSettings;
    }

    public Settings transientSettings() {
        return transientSettings;
    }

    @Nullable
    public Set<String> persistentSettingsToRemove() {
        return persistentSettingsToRemove;
    }

    @Nullable
    public Set<String> transientSettingsToRemove() {
        return transientSettingsToRemove;
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitESClusterUpdateSettingsNode(this, context);
    }
}
