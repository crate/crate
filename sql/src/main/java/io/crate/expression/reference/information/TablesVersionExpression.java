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

package io.crate.expression.reference.information;

import io.crate.Version;
import io.crate.common.collections.Maps;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.reference.ObjectCollectExpression;
import io.crate.metadata.RelationInfo;

import java.util.Map;

public class TablesVersionExpression extends ObjectCollectExpression<RelationInfo> {

    public TablesVersionExpression() {
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(Version.Property.CREATED.toString(),
            new TableDetailedVersionExpression(Version.Property.CREATED));
        childImplementations.put(Version.Property.UPGRADED.toString(),
            new TableDetailedVersionExpression(Version.Property.UPGRADED));
    }

    @Override
    public Map<String, Object> value() {
        Map<String, Object> map = super.value();
        return Maps.mapOrNullIfNullValues(map);
    }

    static class TableDetailedVersionExpression extends ObjectCollectExpression<RelationInfo> {

        TableDetailedVersionExpression(Version.Property property) {
            addChildImplementations(property);
        }

        private void addChildImplementations(Version.Property property) {
            childImplementations.put(Version.CRATEDB_VERSION_KEY, new TableCrateVersionExpression(property));
            childImplementations.put(Version.ES_VERSION_KEY, new TableESVersionExpression(property));
        }

        @Override
        public Map<String, Object> value() {
            Map<String, Object> map = super.value();
            return Maps.mapOrNullIfNullValues(map);
        }
    }

    static class TableCrateVersionExpression extends NestableCollectExpression<RelationInfo, String> {

        private final Version.Property property;
        private String value;

        TableCrateVersionExpression(Version.Property property) {
            this.property = property;
        }

        @Override
        public void setNextRow(RelationInfo references) {
            Version version = TableExpressions.getVersion(references, property);
            value = version == null ? null : version.number();
        }

        @Override
        public String value() {
            return value;
        }
    }

    static class TableESVersionExpression extends NestableCollectExpression<RelationInfo, String> {

        private final Version.Property property;
        private String value;

        TableESVersionExpression(Version.Property property) {
            this.property = property;
        }

        @Override
        public void setNextRow(RelationInfo references) {
            Version version = TableExpressions.getVersion(references, property);
            value = version == null ? null : version.esVersion.toString();
        }

        @Override
        public String value() {
            return value;
        }
    }
}
