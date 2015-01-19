/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.planner.projection.Projection;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.Map;

public class ESCreateTemplateNode extends DDLPlanNode {

    private final String templateName;
    private final String indexMatch;
    private final Map<String, Object> mapping;
    private final Settings indexSettings;
    private final String alias;

    /**
     *
     * @param templateName the name of the template
     * @param indexMatch the name or prefix to match against created indices
     *                   if this template should be applied
     * @param indexSettings settings
     * @param mapping the mapping for the crate default mapping type
     */
    public ESCreateTemplateNode(String templateName,
                                String indexMatch,
                                Settings indexSettings,
                                Map<String, Object> mapping,
                                @Nullable String alias) {
        this.templateName = templateName;
        this.indexMatch = indexMatch;
        this.alias = alias;
        this.mapping = mapping;
        this.indexSettings = indexSettings;
    }

    public String templateName() {
        return templateName;
    }

    public String indexMatch() {
        return indexMatch;
    }

    @Nullable
    public String alias() {
        return alias;
    }

    public Map<String, Object> mapping() {
        return mapping;
    }

    public Settings indexSettings() {
        return indexSettings;
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitESCreateTemplateNode(this, context);
    }

    @Override
    public void addProjection(Projection projection) {
        throw new UnsupportedOperationException("addProjection not supported");
    }
}
