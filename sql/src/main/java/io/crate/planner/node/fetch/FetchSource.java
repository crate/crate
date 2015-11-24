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

package io.crate.planner.node.fetch;

import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Reference;
import io.crate.metadata.ReferenceInfo;

import java.util.Collection;
import java.util.List;

public class FetchSource {

    private final List<ReferenceInfo> partitionedByColumns;
    private final Collection<InputColumn> docIdCols;
    private final Collection<Reference> references;

    public FetchSource(List<ReferenceInfo> partitionedByColumns,
                       Collection<InputColumn> docIdCols,
                       Collection<Reference> references) {
        this.partitionedByColumns = partitionedByColumns;
        this.docIdCols = docIdCols;
        this.references = references;
    }

    public Collection<InputColumn> docIdCols() {
        return docIdCols;
    }

    public List<ReferenceInfo> partitionedByColumns() {
        return partitionedByColumns;
    }

    public Collection<Reference> references() {
        return references;
    }
}
