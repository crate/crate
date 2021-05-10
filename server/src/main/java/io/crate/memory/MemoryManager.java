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

package io.crate.memory;

import io.netty.buffer.ByteBuf;

/**
 * Instances of this interface can be used to allocate `ByteBuf` instances.
 *
 * <ul>
 *     <li>Components that create a MemoryManager are responsible for closing it</li>
 *     <li>Components that use a MemoryManager to allocate ByteBuf instances MUST NOT release them.
 *     The MemoryManager is responsible for releasing the ByteBuf instances it creates</li>
 *  </ul>
 */
public interface MemoryManager extends AutoCloseable {

    /**
     * @return a new ByteBuf with the given capacity.
     *         Consumers of this ByteBuf MUST NOT release them and MUST NOT account for the used memory.
     */
    ByteBuf allocate(int capacity);

    @Override
    void close();
}
