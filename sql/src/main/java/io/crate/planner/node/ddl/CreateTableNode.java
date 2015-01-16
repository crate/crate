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

package io.crate.planner.node.ddl;

import com.google.common.base.Optional;
import io.crate.metadata.TableIdent;
import io.crate.planner.node.PlanNodeVisitor;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.Map;

public class CreateTableNode extends DDLPlanNode {

    private final TableIdent tableIdent;
    private final Settings settings;
    private final Map<String, Object> mapping;


    private final Optional<String> templateName;
    private final Optional<String> templateIndexMatch;

    private CreateTableNode(TableIdent tableIdent, Settings settings,
                            Map<String, Object> mapping,
                            @Nullable String templateName,
                            @Nullable String templateIndexMatch) {
        this.tableIdent = tableIdent;
        this.settings = settings;

        this.mapping = mapping;
        this.templateName = Optional.fromNullable(templateName);
        this.templateIndexMatch = Optional.fromNullable(templateIndexMatch);
    }

    public static CreateTableNode createPartitionedTableNode(TableIdent tableIdent,
                                                       Settings settings,
                                                       Map<String, Object> mapping,
                                                       String templateName,
                                                       String templateIndexMatch) {
        return new CreateTableNode(tableIdent, settings, mapping, templateName, templateIndexMatch);
    }

    public static CreateTableNode createTableNode(TableIdent tableIdent,
                                                  Settings settings,
                                                  Map<String, Object> mapping) {
        return new CreateTableNode(tableIdent, settings, mapping, null, null);
    }

    public TableIdent tableIdent() {
        return tableIdent;
    }

    public Settings settings() {
        return settings;
    }

    public Map<String, Object> mapping() {
        return mapping;
    }

    public Optional<String> templateName() {
        return templateName;
    }

    public Optional<String> templateIndexMatch() {
        return templateIndexMatch;
    }

    public boolean createsPartitionedTable() {
        return templateName.isPresent();
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitCreateTableNode(this, context);
    }
}
