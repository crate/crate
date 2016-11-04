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

package io.crate.metadata.tablefunctions;

import io.crate.data.Bucket;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.table.TableInfo;
import io.crate.data.Input;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.Collection;

/**
 * Interface which needs to be implemented by functions returning whole tables as result.
 */
public interface TableFunctionImplementation extends FunctionImplementation {

    /**
     * Executes the function.
     *
     * @param arguments the argument values
     * @return the resulting table rows as bucket
     */
    Bucket execute(Collection<? extends Input> arguments);

    /**
     * Creates the metadata for the table that is generated upon execution of this function. This is the actual return
     * type of the table function.
     * <p>
     * Note: The result type of the {@link io.crate.metadata.FunctionInfo} that is returned by {@link #info()}
     * is ignored for table functions.
     *
     * @param clusterService the cluster service implementation for retrieving cluster state information
     * @return a table info object representing the actual return type of the function.
     */
    TableInfo createTableInfo(ClusterService clusterService);
}
