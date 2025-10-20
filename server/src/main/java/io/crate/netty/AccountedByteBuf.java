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

package io.crate.netty;

import io.crate.data.breaker.RamAccounting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.WrappedByteBuf;

public class AccountedByteBuf extends WrappedByteBuf {

    private final ByteBuf delegate;
    private final RamAccounting ramAccounting;

    public static ByteBuf of(ByteBuf delegate, RamAccounting ramAccounting) {
        return delegate.isDirect()
            ? delegate
            : new AccountedByteBuf(delegate, ramAccounting);
    }

    private AccountedByteBuf(ByteBuf delegate, RamAccounting ramAccounting) {
        super(delegate);
        this.delegate = delegate;
        this.ramAccounting = ramAccounting;
        ramAccounting.addBytes(delegate.capacity());
    }

    @Override
    public ByteBuf writeBytes(byte[] src, int srcIndex, int length) {
        int oldCapacity = delegate.capacity();
        ByteBuf result = buf.writeBytes(src, srcIndex, length);
        int newCapacity = delegate.capacity();
        ramAccounting.addBytes(newCapacity - oldCapacity);
        return result;
    }
}
