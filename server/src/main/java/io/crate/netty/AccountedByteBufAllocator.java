
package io.crate.netty;

import io.crate.data.breaker.RamAccounting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;

public class AccountedByteBufAllocator implements ByteBufAllocator {

    private final ByteBufAllocator delegate;
    private final RamAccounting ramAccounting;

    public AccountedByteBufAllocator(ByteBufAllocator delegate, RamAccounting ramAccounting) {
        this.delegate = delegate;
        this.ramAccounting = ramAccounting;
    }

    public ByteBuf buffer() {
        return delegate.buffer();
    }

    public ByteBuf buffer(int initialCapacity) {
        return delegate.buffer(initialCapacity);
    }

    public ByteBuf buffer(int initialCapacity, int maxCapacity) {
        return delegate.buffer(initialCapacity, maxCapacity);
    }

    public ByteBuf ioBuffer() {
        return delegate.ioBuffer();
    }

    public ByteBuf ioBuffer(int initialCapacity) {
        return delegate.ioBuffer(initialCapacity);
    }

    public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
        return delegate.ioBuffer(initialCapacity, maxCapacity);
    }

    public ByteBuf heapBuffer() {
        ByteBuf heapBuffer = delegate.heapBuffer();
        return AccountedByteBuf.of(heapBuffer, ramAccounting);
    }

    public ByteBuf heapBuffer(int initialCapacity) {
        ByteBuf heapBuffer = delegate.heapBuffer(initialCapacity);
        return AccountedByteBuf.of(heapBuffer, ramAccounting);
    }

    public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
        ByteBuf heapBuffer = delegate.heapBuffer(initialCapacity, maxCapacity);
        return AccountedByteBuf.of(heapBuffer, ramAccounting);
    }

    public ByteBuf directBuffer() {
        return delegate.directBuffer();
    }

    public ByteBuf directBuffer(int initialCapacity) {
        return delegate.directBuffer(initialCapacity);
    }

    public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
        return delegate.directBuffer(initialCapacity, maxCapacity);
    }

    public CompositeByteBuf compositeBuffer() {
        return delegate.compositeBuffer();
    }

    public CompositeByteBuf compositeBuffer(int maxNumComponents) {
        return delegate.compositeBuffer(maxNumComponents);
    }

    public CompositeByteBuf compositeHeapBuffer() {
        return delegate.compositeHeapBuffer();
    }

    public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
        return delegate.compositeHeapBuffer(maxNumComponents);
    }

    public CompositeByteBuf compositeDirectBuffer() {
        return delegate.compositeDirectBuffer();
    }

    public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
        return delegate.compositeDirectBuffer(maxNumComponents);
    }

    public boolean isDirectBufferPooled() {
        return delegate.isDirectBufferPooled();
    }

    public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
        return delegate.calculateNewCapacity(minNewCapacity, maxCapacity);
    }

}
