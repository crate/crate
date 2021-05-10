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

package io.crate.execution.engine.collect.files;

import org.elasticsearch.ResourceNotFoundException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class SqlFeatures {

    private static final int NUM_COLS = 7;

    public static Iterable<SqlFeatureContext> loadFeatures() throws IOException {
        try (InputStream sqlFeatures = SqlFeatures.class.getResourceAsStream("/sql_features.tsv")) {
            if (sqlFeatures == null) {
                throw new ResourceNotFoundException("sql_features.tsv file not found");
            }
            ArrayList<SqlFeatureContext> featuresList = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(sqlFeatures, StandardCharsets.UTF_8))) {
                String next;
                while ((next = reader.readLine()) != null) {
                    List<String> parts = List.of(next.split("\t", NUM_COLS));
                    var ctx = new SqlFeatureContext(parts.get(0),
                        parts.get(1),
                        parts.get(2),
                        parts.get(3),
                        parts.get(4).equals("YES"),
                        parts.get(5).isEmpty() ? null : parts.get(5),
                        parts.get(6).isEmpty() ? null : parts.get(6));
                    featuresList.add(ctx);
                }
            }
            return featuresList;
        }
    }
}
