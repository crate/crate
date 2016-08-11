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

import javax.annotation.Nullable;

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

    public static Builder builder() {
        return new Builder();
    }

    static class Builder {

        private String featureId;
        private String featureName;
        private String subFeatureId;
        private String subFeatureName;
        private boolean isSupported;
        private String isVerifiedBy;
        private String comments;

        public Builder featureId(String featureId) {
            this.featureId = featureId;
            return this;
        }

        public Builder featureName(String featureName) {
            this.featureName = featureName;
            return this;
        }

        public Builder subFeatureId(String subFeatureId) {
            this.subFeatureId = subFeatureId;
            return this;
        }

        public Builder subFeatureName(String subFeatureName) {
            this.subFeatureName = subFeatureName;
            return this;
        }

        public Builder isSupported(boolean isSupported) {
            this.isSupported = isSupported;
            return this;
        }

        public Builder isVerifiedBy(String isVerifiedBy) {
            this.isVerifiedBy = isVerifiedBy;
            return this;
        }

        public Builder comments(String comments) {
            this.comments = comments;
            return this;
        }

        public SqlFeatureContext build() {
            return new SqlFeatureContext(featureId, featureName,
                subFeatureId, subFeatureName,
                isSupported, isVerifiedBy,
                comments);
        }
    }
}
