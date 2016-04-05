/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.testing.plugin;

import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.testing.SleepScalarFunction;
import org.elasticsearch.plugins.Plugin;

public class CrateTestingPlugin extends Plugin {

    @Override
    public String name() {
        return "crate-testing-plugin";
    }

    @Override
    public String description() {
        return "A plugin that implements functions used for testing.";
    }

    public void onModule(ScalarFunctionModule module) {
        module.register(new SleepScalarFunction());
    }
}
