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

package io.crate.expression.reference.partitioned;

import io.crate.common.collections.Maps;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.reference.ObjectCollectExpression;
import io.crate.metadata.PartitionInfo;
import org.elasticsearch.Version;

import java.util.Map;

public class PartitionsVersionExpression extends ObjectCollectExpression<PartitionInfo> {

    public PartitionsVersionExpression() {
        addChildImplementations();
    }

    private void addChildImplementations() {
        childImplementations.put(Version.Property.CREATED.toString(),
                                 NestableCollectExpression.withNullableProperty(PartitionInfo::versionCreated,
                                                                                Version::externalNumber));
        childImplementations.put(Version.Property.UPGRADED.toString(),
                                 NestableCollectExpression.withNullableProperty(PartitionInfo::versionUpgraded,
                                                                                Version::externalNumber));
    }

    @Override
    public Map<String, Object> value() {
        Map<String, Object> map = super.value();
        return Maps.mapOrNullIfNullValues(map);
    }
}
