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

import org.locationtech.spatial4j.shape.Point;

public class SummitsContext {

    public final String mountain;
    public final Integer height;
    public final Integer prominence;
    public final Point coordinates;
    public final String range;
    public final String region;
    public final String classification;
    public final String country;
    public final Integer firstAscent;

    SummitsContext(String mountain,
                   Integer height,
                   Integer prominence,
                   Point coordinates,
                   String region,
                   String classification,
                   String range,
                   String country,
                   Integer firstAscent) {
        this.mountain = mountain;
        this.height = height;
        this.prominence = prominence;
        this.coordinates = coordinates;
        this.range = range;
        this.region = region;
        this.classification = classification;
        this.country = country;
        this.firstAscent = firstAscent;
    }

    public String mountain() {
        return mountain;
    }

    public Integer height() {
        return height;
    }

    public Integer prominence() {
        return prominence;
    }

    public Point coordinates() {
        return coordinates;
    }

    public String range() {
        return range;
    }

    public String region() {
        return region;
    }

    public String classification() {
        return classification;
    }

    public String country() {
        return country;
    }

    public Integer firstAscent() {
        return firstAscent;
    }
}
