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

package io.crate.metadata;

import io.crate.Version;
import org.elasticsearch.common.collect.MapBuilder;

import java.util.HashMap;
import java.util.Map;

public final class IndexMappings {

    public static final String DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME = "Murmur3";
    public static final String VERSION_STRING = "version";

    public static final Map<String, Object> DEFAULT_TABLE_MAPPING = createDefaultTableMapping();

    public static void putDefaultSettingsToMeta(Map<String, Object> metaMap) {
        // set the created version
        IndexMappings.putVersionToMap(metaMap, Version.Property.CREATED, Version.CURRENT);
    }

    private static void putVersionToMap(Map<String, Object> metaMap, Version.Property key, Version version) {
        Map<String, Object> versionMap = (Map<String, Object>) metaMap.get(VERSION_STRING);
        if (versionMap == null) {
            versionMap = new HashMap<>(1);
            metaMap.put(VERSION_STRING, versionMap);
        }
        versionMap.put(key.toString(), Version.toMap(version));
    }

    private static Map<String, Object> createDefaultTableMapping() {
        Map<String, Object> metaMap = new HashMap<>(1);
        putDefaultSettingsToMeta(metaMap);
        return MapBuilder.<String, Object>newMapBuilder().put("_meta", metaMap).map();
    }
}
