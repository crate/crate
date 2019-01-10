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

package org.elasticsearch.index.engine;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Deprecated as not used in 6.0, should be removed in 7.0
 * Still exists for bwc in serializing/deserializing from
 * 5.x nodes
 */
@Deprecated
public class DeleteFailedEngineException extends EngineException {
    public DeleteFailedEngineException(StreamInput in) throws IOException{
        super(in);
    }
}
