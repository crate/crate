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

package io.crate.operation.collect.files;

import com.google.common.base.Splitter;
import org.elasticsearch.ResourceNotFoundException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class SqlFeaturesIterable implements Iterable<SqlFeatureContext> {

    private static SqlFeaturesIterable instance = null;
    private static List<SqlFeatureContext> featuresList;
    private static final Splitter TAB_SPLITTER = Splitter.on("\t");

    public static SqlFeaturesIterable getInstance() {
        if (instance == null) {
            instance = new SqlFeaturesIterable();
        }
        return instance;
    }

    private SqlFeaturesIterable() {
        loadContextList();
    }

    private void loadContextList() {
        try (InputStream sqlFeatures = SqlFeaturesIterable.class.getResourceAsStream("/sql_features.tsv");
             BufferedReader reader = new BufferedReader(new InputStreamReader(sqlFeatures, StandardCharsets.UTF_8))) {
            if (sqlFeatures == null) {
                throw new ResourceNotFoundException("sql_features.tsv file not found");
            }
            featuresList = new ArrayList<>();
            SqlFeatureContext ctx;
            String next;
            while ((next = reader.readLine()) != null) {
                List<String> parts = TAB_SPLITTER.splitToList(next);
                ctx = SqlFeatureContext.builder()
                        .featureId(parts.get(0))
                        .featureName(parts.get(1))
                        .subFeatureId(parts.get(2))
                        .subFeatureName(parts.get(3))
                        .isSupported(parts.get(4).equals("YES"))
                        .isVerifiedBy(parts.get(5).isEmpty() ? null : parts.get(5))
                        .comments(parts.get(6).isEmpty() ? null : parts.get(6))
                        .build();
                featuresList.add(ctx);
            }
        } catch (IOException e) {
        }
    }

    @Override
    public Iterator<SqlFeatureContext> iterator() {
        return featuresList.iterator();
    }
}
