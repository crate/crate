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

import javax.annotation.Nullable;
import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SqlFeaturesIterable implements Iterable<SqlFeatureContext> {

    private static SqlFeaturesIterable instance = null;
    private static List<SqlFeatureContext> featuresList;
    private static final Splitter TAB_SPLITTER = Splitter.on("\t");
    private static final URL SQL_FEATURES = SqlFeaturesIterable.class.getResource("/sql_features.tsv");
    private BufferedReader reader;

    public static SqlFeaturesIterable getInstance() {
        if (instance == null) {
            instance = new SqlFeaturesIterable();
        }
        return instance;
    }

    private SqlFeaturesIterable() {
        if (SQL_FEATURES == null) {
            throw new ResourceNotFoundException("sql_features.tsv file not found");
        }
        loadContextList();
    }

    private void loadContextList() {
        featuresList = new ArrayList<>();
        Path features = null;
        try {
            features = Paths.get(SQL_FEATURES.toURI());
        } catch (URISyntaxException e) {
        }
        reader = getReader(features);
        SqlFeatureContext ctx;
        String next;
        while ((next = nextLine()) != null) {
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
        close();
    }

    private BufferedReader getReader(Path features) {
        try {
            return Files.newBufferedReader(features, StandardCharsets.UTF_8);
        } catch (IOException e) {
            return null;
        }
    }

    @Nullable
    private String nextLine() {
        try {
            return reader.readLine();
        } catch (IOException e) {
            return null;
        }
    }

    private void close() {
        try {
            reader.close();
        } catch (IOException e) {
        }
    }

    @Override
    public Iterator<SqlFeatureContext> iterator() {
        return featuresList.iterator();
    }
}
