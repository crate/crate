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

package io.crate.jobs;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

public class JobModule extends AbstractModule {

    private static final String JOB_KEEP_ALIVE = "jobs.keep_alive_timeout";
    private final TimeValue keepAlive;

    public JobModule(Settings settings) {
        keepAlive = settings.getAsTime(JOB_KEEP_ALIVE, timeValueSeconds(0));
    }

    @Override
    protected void configure() {
        bind(JobContextService.class).asEagerSingleton();
        bind(TimeValue.class).annotatedWith(JobContextService.JobKeepAlive.class).toInstance(keepAlive);
        bind(TimeValue.class).annotatedWith(JobContextService.ReaperInterval.class).toInstance(timeValueMinutes(1));
        bind(Reaper.class).to(LocalReaper.class).asEagerSingleton();

        bind(KeepAliveTimers.class).asEagerSingleton();
    }
}
