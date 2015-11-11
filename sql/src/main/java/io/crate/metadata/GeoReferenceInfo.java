/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.metadata;

import io.crate.types.DataTypes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;

public class GeoReferenceInfo extends ReferenceInfo {

    public static class Builder {

        private ReferenceIdent ident;
        private String tree = "geohash"; // reflects the default value
        private @Nullable Integer treeLevels;
        private @Nullable Double distanceErrorPct;


        public Builder ident(ReferenceIdent ident) {
            this.ident = ident;
            return this;
        }

        public Builder treeLevels(Integer treeLevels) {
            this.treeLevels = treeLevels;
            return this;
        }

        public Builder distanceErrorPct(Double errorPct) {
            this.distanceErrorPct = errorPct;
            return this;
        }

        public Builder geoTree(String tree) {
            this.tree = tree;
            return this;
        }

        public GeoReferenceInfo build() {
            Preconditions.checkNotNull(ident, "ident is null");
            Preconditions.checkNotNull(tree, "tree is null");
            return new GeoReferenceInfo(ident, tree, treeLevels, distanceErrorPct);
        }

    }

    private String geoTree;
    private @Nullable Integer treeLeveles;
    private @Nullable Double distanceErrorPct;

    public GeoReferenceInfo(ReferenceIdent ident,
                            String tree,
                            @Nullable Integer treeLeveles,
                            @Nullable Double distanceErrorPct) {
        super(ident, RowGranularity.DOC, DataTypes.GEO_SHAPE);
        this.geoTree = tree;
        this.treeLeveles = treeLeveles;
        this.distanceErrorPct = distanceErrorPct;
    }

    public String geoTree() {
        return geoTree;
    }

    @Nullable
    public Integer treeLevels() {
        return treeLeveles;
    }

    @Nullable
    public Double distanceErrorPct() {
        return distanceErrorPct;
    }
}
