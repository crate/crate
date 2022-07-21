/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.testing;

import org.assertj.core.api.AbstractAssert;
import org.elasticsearch.common.settings.Settings;

public final class SettingsAssert extends AbstractAssert<SettingsAssert, Settings> {

    public SettingsAssert(Settings actual) {
        super(actual, SettingsAssert.class);
    }

    public SettingsAssert hasEntry(String key, String value) {
        isNotNull();
        if (actual.keySet().contains(key) == false) {
            failWithMessage("Expected Settings to have a key: [%s]", key);
        }
        String actualValue = actual.get(key);
        if (actualValue.equals(value) == false) {
            failWithMessage("Expected Settings to have a key [%s] with value [%s] but value was [%s]",
                            key, value, actualValue);
        }
        return this;
    }

    public SettingsAssert hasKey(String key) {
        isNotNull();
        if (actual.keySet().contains(key) == false) {
            failWithMessage("Expected Settings to have a key: [%s]", key);
        }
        return this;
    }
}
