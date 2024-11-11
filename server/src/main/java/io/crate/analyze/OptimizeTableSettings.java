/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import java.util.Map;

import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

public final class OptimizeTableSettings {

    private OptimizeTableSettings() {}

    public static final Setting<Integer> MAX_NUM_SEGMENTS =
        Setting.intSetting(
            "max_num_segments",
            ForceMergeRequest.Defaults.MAX_NUM_SEGMENTS,
            ForceMergeRequest.Defaults.MAX_NUM_SEGMENTS,
            Integer.MAX_VALUE);

    public static final Setting<Boolean> ONLY_EXPUNGE_DELETES =
        Setting.boolSetting("only_expunge_deletes", ForceMergeRequest.Defaults.ONLY_EXPUNGE_DELETES);

    public static final Setting<Boolean> FLUSH = Setting.boolSetting("flush", ForceMergeRequest.Defaults.FLUSH);

    public static final Setting<Boolean> UPGRADE_SEGMENTS = Setting.boolSetting("upgrade_segments", false, Property.Deprecated);

    public static final Map<String, Setting<?>> SUPPORTED_SETTINGS = Map.of(
        MAX_NUM_SEGMENTS.getKey(), MAX_NUM_SEGMENTS,
        ONLY_EXPUNGE_DELETES.getKey(), ONLY_EXPUNGE_DELETES,
        FLUSH.getKey(), FLUSH,
        UPGRADE_SEGMENTS.getKey(), UPGRADE_SEGMENTS
    );
}
