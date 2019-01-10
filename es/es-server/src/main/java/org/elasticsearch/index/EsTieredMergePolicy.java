/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index;

import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;

import java.io.IOException;
import java.util.Map;

/**
 * Wrapper around {@link TieredMergePolicy} which doesn't respect
 * {@link TieredMergePolicy#setMaxMergedSegmentMB(double)} on forced merges.
 * See https://issues.apache.org/jira/browse/LUCENE-7976.
 */
final class EsTieredMergePolicy extends FilterMergePolicy {

    final TieredMergePolicy regularMergePolicy;
    final TieredMergePolicy forcedMergePolicy;

    EsTieredMergePolicy() {
        super(new TieredMergePolicy());
        regularMergePolicy = (TieredMergePolicy) in;
        forcedMergePolicy = new TieredMergePolicy();
        forcedMergePolicy.setMaxMergedSegmentMB(Double.POSITIVE_INFINITY); // unlimited
    }

    @Override
    public MergeSpecification findForcedMerges(SegmentInfos infos, int maxSegmentCount,
            Map<SegmentCommitInfo, Boolean> segmentsToMerge, MergeContext mergeContext) throws IOException {
        return forcedMergePolicy.findForcedMerges(infos, maxSegmentCount, segmentsToMerge, mergeContext);
    }

    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos infos, MergeContext mergeContext) throws IOException {
        return forcedMergePolicy.findForcedDeletesMerges(infos, mergeContext);
    }

    public void setForceMergeDeletesPctAllowed(double forceMergeDeletesPctAllowed) {
        regularMergePolicy.setForceMergeDeletesPctAllowed(forceMergeDeletesPctAllowed);
        forcedMergePolicy.setForceMergeDeletesPctAllowed(forceMergeDeletesPctAllowed);
    }

    public double getForceMergeDeletesPctAllowed() {
        return forcedMergePolicy.getForceMergeDeletesPctAllowed();
    }

    public void setFloorSegmentMB(double mbFrac) {
        regularMergePolicy.setFloorSegmentMB(mbFrac);
        forcedMergePolicy.setFloorSegmentMB(mbFrac);
    }

    public double getFloorSegmentMB() {
        return regularMergePolicy.getFloorSegmentMB();
    }

    public void setMaxMergeAtOnce(int maxMergeAtOnce) {
        regularMergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
        forcedMergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
    }

    public int getMaxMergeAtOnce() {
        return regularMergePolicy.getMaxMergeAtOnce();
    }

    public void setMaxMergeAtOnceExplicit(int maxMergeAtOnceExplicit) {
        regularMergePolicy.setMaxMergeAtOnceExplicit(maxMergeAtOnceExplicit);
        forcedMergePolicy.setMaxMergeAtOnceExplicit(maxMergeAtOnceExplicit);
    }

    public int getMaxMergeAtOnceExplicit() {
        return forcedMergePolicy.getMaxMergeAtOnceExplicit();
    }

    // only setter that must NOT delegate to the forced merge policy
    public void setMaxMergedSegmentMB(double mbFrac) {
        regularMergePolicy.setMaxMergedSegmentMB(mbFrac);
    }

    public double getMaxMergedSegmentMB() {
        return regularMergePolicy.getMaxMergedSegmentMB();
    }

    public void setSegmentsPerTier(double segmentsPerTier) {
        regularMergePolicy.setSegmentsPerTier(segmentsPerTier);
        forcedMergePolicy.setSegmentsPerTier(segmentsPerTier);
    }

    public double getSegmentsPerTier() {
        return regularMergePolicy.getSegmentsPerTier();
    }

    public void setDeletesPctAllowed(double deletesPctAllowed) {
        regularMergePolicy.setDeletesPctAllowed(deletesPctAllowed);
        forcedMergePolicy.setDeletesPctAllowed(deletesPctAllowed);
    }

    public double getDeletesPctAllowed() {
        return regularMergePolicy.getDeletesPctAllowed();
    }
}
