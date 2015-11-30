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

package io.crate.sql.tree;

import javax.annotation.Nullable;
import java.util.Objects;

public class CopyQueryTo extends CopyTo {

    private final Query subQuery;

    public CopyQueryTo(Query subQuery,
                       boolean directoryUri,
                       Expression targetUri,
                       @Nullable GenericProperties genericProperties) {
        super(directoryUri, targetUri, genericProperties);
        this.subQuery = subQuery;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CopyQueryTo that = (CopyQueryTo) o;
        return directoryUri == that.directoryUri &&
               Objects.equals(subQuery, that.subQuery) &&
               Objects.equals(targetUri, that.targetUri) &&
               Objects.equals(genericProperties, that.genericProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subQuery, directoryUri, targetUri, genericProperties);
    }

    @Override
    public String toString() {
        return "CopyQueryTo{" +
               "subQuery=" + subQuery +
               ", directoryUri=" + directoryUri +
               ", targetUri=" + targetUri +
               ", genericProperties=" + genericProperties +
               '}';
    }
}
