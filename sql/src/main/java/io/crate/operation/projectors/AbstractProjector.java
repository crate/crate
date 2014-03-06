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

package io.crate.operation.projectors;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;

import javax.annotation.Nullable;

public abstract class AbstractProjector implements Projector {

    protected Optional<Projector> downStream;
    protected final Input<?>[] inputs;
    protected final CollectExpression<?>[] collectExpressions;

    public AbstractProjector() {
        inputs = new Input[0];
        collectExpressions = new CollectExpression[0];
    }

    public AbstractProjector(Input<?>[] inputs, CollectExpression<?>[] collectExpressions) {
        this(inputs, collectExpressions, null);
    }

    public AbstractProjector(Input<?>[] inputs, CollectExpression<?>[] collectExpressions, @Nullable Projector downStream) {
        Preconditions.checkArgument(inputs != null);
        Preconditions.checkArgument(collectExpressions != null);
        this.inputs = inputs;
        this.collectExpressions = collectExpressions;
        this.downStream = Optional.fromNullable(downStream);
    }

    @Override
    public void setDownStream(Projector downStream) {
        this.downStream = Optional.of(downStream);
    }
}
