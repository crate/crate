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

package io.crate.es.common.geo;

import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;

import java.util.List;

/**
 * Extends spatial4j ShapeCollection for points_only shape indexing support
 */
public class XShapeCollection<S extends Shape> extends ShapeCollection<S> {

  private boolean pointsOnly = false;

  public XShapeCollection(List<S> shapes, SpatialContext ctx) {
    super(shapes, ctx);
  }

  public boolean pointsOnly() {
    return this.pointsOnly;
  }

  public void setPointsOnly(boolean pointsOnly) {
    this.pointsOnly = pointsOnly;
  }
}
