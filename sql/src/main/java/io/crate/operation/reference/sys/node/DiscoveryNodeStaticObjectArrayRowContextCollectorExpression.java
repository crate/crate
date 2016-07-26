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

package io.crate.operation.reference.sys.node;

import io.crate.operation.reference.RowCollectNestedObjectExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class DiscoveryNodeStaticObjectArrayRowContextCollectorExpression extends ObjectArrayRowContextCollectorExpression<DiscoveryNodeContext> {

    private final AtomicBoolean initialized = new AtomicBoolean(false);
    protected final List<RowCollectNestedObjectExpression<DiscoveryNodeContext>> childImplementations = new ArrayList<>();

    @Override
    protected List<RowCollectNestedObjectExpression<DiscoveryNodeContext>> getChildImplementations() {
        if (!initialized.getAndSet(true)) {
            addChildImplementations();
        }
        return childImplementations;
    }

    @Override
    public Object[] value() {
        return row.isEmpty() ? null : super.value();
    }

    public abstract void addChildImplementations();

}
