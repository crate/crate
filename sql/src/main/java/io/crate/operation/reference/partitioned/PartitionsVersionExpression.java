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

package io.crate.operation.reference.partitioned;

import io.crate.Version;
import io.crate.core.collections.Maps;
import io.crate.metadata.PartitionInfo;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.operation.reference.RowCollectNestedObjectExpression;
import io.crate.operation.reference.information.TableExpressions;

import java.util.Map;

public class PartitionsVersionExpression extends RowCollectNestedObjectExpression<PartitionInfo> {

    public PartitionsVersionExpression() {
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(Version.Property.CREATED.toString(),
            new PartitionDetailedVersionExpression(Version.Property.CREATED));
        childImplementations.put(Version.Property.UPGRADED.toString(),
            new PartitionDetailedVersionExpression(Version.Property.UPGRADED));
    }

    @Override
    public Map<String, Object> value() {
        Map<String, Object> map = super.value();
        return Maps.mapOrNullIfNullValues(map);
    }

    static class PartitionDetailedVersionExpression extends RowCollectNestedObjectExpression<PartitionInfo> {

        PartitionDetailedVersionExpression(Version.Property property) {
            addChildImplementations(property);
        }

        private void addChildImplementations(Version.Property property) {
            childImplementations.put(Version.CRATEDB_VERSION_KEY, new PartitionCrateVersionExpression(property));
            childImplementations.put(Version.ES_VERSION_KEY, new PartitionESVersionExpression(property));
        }

        @Override
        public Map<String, Object> value() {
            Map<String, Object> map = super.value();
            return Maps.mapOrNullIfNullValues(map);
        }
    }

    static class PartitionCrateVersionExpression extends RowContextCollectorExpression<PartitionInfo, Object> {

        private final Version.Property property;

        PartitionCrateVersionExpression(Version.Property property) {
            this.property = property;
        }

        @Override
        public Object value() {
            Version version = TableExpressions.getVersion(row, property);
            return version == null ? null : version.number();
        }
    }

    static class PartitionESVersionExpression extends RowContextCollectorExpression<PartitionInfo, Object> {

        private final Version.Property property;

        PartitionESVersionExpression(Version.Property property) {
            this.property = property;
        }

        @Override
        public Object value() {
            Version version = TableExpressions.getVersion(row, property);
            return version == null ? null : version.esVersion.toString();
        }
    }
}
