/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.reference.sys.shard.blob;

import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.shard.blob.BlobShardReferenceImplementation;
import io.crate.operation.reference.sys.shard.SysShardExpression;
import org.apache.lucene.util.BytesRef;

public class BlobShardSchemaNameExpression extends SysShardExpression<BytesRef> implements BlobShardReferenceImplementation {

    public static final String NAME = "schema_name";
    public static final BytesRef BLOB_SCHEMA_NAME = new BytesRef(BlobSchemaInfo.NAME);

    protected BlobShardSchemaNameExpression() {
    }

    @Override
    public BytesRef value() {
        return BLOB_SCHEMA_NAME;
    }
}
