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

package io.crate.copy.s3;

import io.crate.execution.engine.collect.files.FileInput;
import io.crate.execution.engine.collect.files.FileInputFactory;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class S3FileInputFactory extends FileInputFactory {

    public static final String NAME = "s3";
    public static Map<String, Set<Object>> requiredWithOptions = Map.of("protocol", Set.of("http", "https"));

    @Override
    public FileInput doCreate(URI uri, Map<String, Object> validatedWithOptions) {
        return new S3FileInput(uri, validatedWithOptions);
    }

    @Override
    public Map<String, Object> validate(Map<String, Object> allWithClauseOptions) throws IllegalArgumentException{
        Map<String, Object> validated = new HashMap<>();
        for (var e : requiredWithOptions.entrySet()) {
            var givenOption = allWithClauseOptions.get(e.getKey());
            if (givenOption != null) {
                if (e.getValue().contains(givenOption)) {
                    validated.put(e.getKey(), givenOption);
                }
            }
        }
        return Collections.unmodifiableMap(validated);
    }
}
