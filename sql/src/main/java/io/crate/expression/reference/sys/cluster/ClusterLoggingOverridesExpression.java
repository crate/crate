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

package io.crate.expression.reference.sys.cluster;

import io.crate.metadata.ReferenceImplementation;
import io.crate.expression.reference.NestedObjectExpression;
import io.crate.expression.reference.sys.SysObjectArrayReference;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ClusterLoggingOverridesExpression extends SysObjectArrayReference {

    public static final String NAME = "logger";
    private final ClusterService clusterService;

    public ClusterLoggingOverridesExpression(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    protected List<NestedObjectExpression> getChildImplementations() {
        Map<String, String> settingsMap = clusterService.state().metaData().settings().getAsMap();
        List<NestedObjectExpression> loggerExpressions = new ArrayList<>();
        for (final Map.Entry<String, String> entry : settingsMap.entrySet()) {
            if (entry.getKey().startsWith("logger.")) {
                loggerExpressions.add(new ClusterLoggingOverridesChildExpression(entry));
            }
        }
        return loggerExpressions;
    }

    public static class ClusterLoggingOverridesChildExpression extends NestedObjectExpression {

        public static final String NAME = "name";
        public static final String LEVEL = "level";

        protected ClusterLoggingOverridesChildExpression(final Map.Entry<String, String> setting) {
            childImplementations.put(NAME, new ReferenceImplementation<BytesRef>() {
                @Override
                public BytesRef value() {
                    return new BytesRef(setting.getKey());
                }
            });
            childImplementations.put(LEVEL, new ReferenceImplementation<BytesRef>() {
                @Override
                public BytesRef value() {
                    return new BytesRef(setting.getValue().toUpperCase(Locale.ENGLISH));
                }
            });

        }
    }
}
