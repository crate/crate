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

package io.crate.expression.reference.sys.check.node;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class SysNodeChecksModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(SysNodeChecks.class).asEagerSingleton();

        MapBinder<Integer, SysNodeCheck> b = MapBinder.newMapBinder(binder(), Integer.class, SysNodeCheck.class);

        // Node checks ordered by ID. New ID must be max ID + 1 and must not be reused.
        b.addBinding(RecoveryExpectedNodesSysCheck.ID).to(RecoveryExpectedNodesSysCheck.class);
        b.addBinding(RecoveryAfterNodesSysCheck.ID).to(RecoveryAfterNodesSysCheck.class);
        b.addBinding(RecoveryAfterTimeSysCheck.ID).to(RecoveryAfterTimeSysCheck.class);
        b.addBinding(HighDiskWatermarkNodesSysCheck.ID).to(HighDiskWatermarkNodesSysCheck.class);
        b.addBinding(LowDiskWatermarkNodesSysCheck.ID).to(LowDiskWatermarkNodesSysCheck.class);
        b.addBinding(FloodStageDiskWatermarkNodesSysCheck.ID).to(FloodStageDiskWatermarkNodesSysCheck.class);
    }
}
