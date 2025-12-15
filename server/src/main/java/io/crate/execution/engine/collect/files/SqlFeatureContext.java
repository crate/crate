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

import org.jspecify.annotations.Nullable;

public class SqlFeatureContext {

    public final String featureId;
    public final String featureName;
    public final String subFeatureId;
    public final String subFeatureName;
    public final boolean isSupported;
    @Nullable
    public final String isVerifiedBy;
    @Nullable
    public final String comments;

    public SqlFeatureContext(String featureId, String featureName,
                             String subFeatureId, String subFeatureName,
                             boolean isSupported, String isVerifiedBy,
                             String comments) {
        this.featureId = featureId;
        this.featureName = featureName;
        this.subFeatureId = subFeatureId;
        this.subFeatureName = subFeatureName;
        this.isSupported = isSupported;
        this.isVerifiedBy = isVerifiedBy;
        this.comments = comments;
    }

    public String getFeatureId() {
        return featureId;
    }

    public String getFeatureName() {
        return featureName;
    }

    public String getSubFeatureId() {
        return subFeatureId;
    }

    public String getSubFeatureName() {
        return subFeatureName;
    }

    public boolean isSupported() {
        return isSupported;
    }

    @Nullable
    public String getIsVerifiedBy() {
        return isVerifiedBy;
    }

    @Nullable
    public String getComments() {
        return comments;
    }
}
