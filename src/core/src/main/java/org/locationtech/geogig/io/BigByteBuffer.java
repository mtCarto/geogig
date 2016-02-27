package org.locationtech.geogig.io;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

public class BigByteBuffer extends BigBuffer {

    private ByteBufferStore store;

    /**
     * Note: do not use it but through {@link #getTmpBuff()}
     */
    private byte[] tmpBuff = new byte[8];

    public BigByteBuffer(ByteBufferStore store, long maxFileSize) {
        this(store, 256 * 1024 * 1024, -1, 0, maxFileSize, new AtomicLong(maxFileSize), false, 0L);
    }

    public BigByteBuffer(final ByteBufferStore store, final int partitionSize,
            final boolean growAutomatically) {
        this(store, partitionSize, -1, 0, partitionSize, new AtomicLong(partitionSize),
                growAutomatically, 0L);
    }

    BigByteBuffer(ByteBufferStore store, int partitionSize, long mark, long pos, long lim,
            AtomicLong cap, boolean growAutomatically, long offset) {
        super(mark, pos, lim, cap, growAutomatically, partitionSize, offset);
        this.store = store;
    }

    public void discard() {
        ByteBufferStore store = this.store;
        this.store = null;
        if (store != null) {
            store.discard();
        }
    }

    public BigByteBuffer putInt(final int v) {
        final long p = position();
        putInt(p, v);
        checkState(p == position());
        position(p + 4);
        return this;
    }

    public BigByteBuffer putInt(final long offset, final int v) {
        ByteBuffer partition = getPartitionAt(offset);
        if (partition.remaining() > 3) {
            partition.putInt(v);
            return this;
        }
        // hit a partition edge
        byte[] buff = this.getTmpBuff();
        buff[0] = (byte) ((v >>> 24) & 0xFF);
        buff[1] = (byte) ((v >>> 16) & 0xFF);
        buff[2] = (byte) ((v >>> 8) & 0xFF);
        buff[3] = (byte) ((v >>> 0) & 0xFF);
        put(offset, buff, 0, 4);
        return this;
    }

    public BigByteBuffer putLong(final long v) {
        final long p = position();
        putLong(p, v);
        checkState(p == position());
        position(p + 8);
        return this;
    }

    public void putLong(final long offset, final long v) {
        ByteBuffer partition = getPartitionAt(offset);
        if (partition.remaining() > 7) {
            partition.putLong(v);
            return;
        }
        // hit a partition edge
        byte[] writeBuff = this.getTmpBuff();
        writeBuff[0] = (byte) (v >>> 56);
        writeBuff[1] = (byte) (v >>> 48);
        writeBuff[2] = (byte) (v >>> 40);
        writeBuff[3] = (byte) (v >>> 32);
        writeBuff[4] = (byte) (v >>> 24);
        writeBuff[5] = (byte) (v >>> 16);
        writeBuff[6] = (byte) (v >>> 8);
        writeBuff[7] = (byte) (v >>> 0);

        put(offset, writeBuff, 0, 8);
    }

    public long getLong() {
        final long p = position();
        final long l = getLong(p);
        checkState(p == position());
        position(p + 8);
        return l;
    }

    /**
     * Absolute get long, doensn't change the buffer's state
     */
    public long getLong(final long offset) {
        ByteBuffer partition = getPartitionAt(offset);
        if (partition.remaining() > 7) {
            return partition.getLong();
        }
        // hit a partition edge
        byte[] buff = this.getTmpBuff();
        get(offset, buff, 0, 8);
        final long l = Longs.fromByteArray(buff);
        return l;
    }

    public float getFloat() {
        final long p = position();
        final float f = getFloat(p);
        checkState(p == position());
        position(p + 4);
        return f;
    }

    /**
     * Absolute get float, doensn't change the buffer's state
     */
    public float getFloat(final long offset) {
        float f = Float.intBitsToFloat(getInt(offset));
        return f;
    }

    public double getDouble() {
        final long p = position();
        final double d = getDouble(p);
        checkState(p == position());
        position(p + 4);
        return d;
    }

    /**
     * Absolute get double, doensn't change the buffer's state
     */
    public double getDouble(final long offset) {
        double d = Double.longBitsToDouble(getLong(offset));
        return d;
    }

    /**
     * Relative get, reads the byte at the current {@link #position()} and increments the position.
     * 
     * @throws java.nio.BufferUnderflowException
     */
    public final byte get() {
        final long p = position();
        byte b = get(p);
        position(p + 1);
        return b;
    }

    /**
     * Relative get, reads the byte at the given {@code offset} without changing this buffer's state
     */
    public final byte get(long offset) {
        checkOverflow(offset, 1);
        ByteBuffer partition = getPartitionAt(offset);
        byte b = partition.get();
        return b;
    }

    /**
     * Returns the {@link ByteBuffer} partition that belongs to the given {@code position}, with its
     * position set at the offset that resolves to {@code position}, and its limit set to its
     * capacity.
     */
    protected ByteBuffer getPartitionAt(final long position) {
        final long absolutePosition = super.offset + position;
        final int partitionSize = super.partitionSize;
        final long partitionIndex = absolutePosition / partitionSize;
        final long partitionOffset = absolutePosition % partitionSize;
        Preconditions.checkState(partitionIndex < Integer.MAX_VALUE);
        Preconditions.checkState(partitionOffset < Integer.MAX_VALUE);

        ByteBuffer partition = store.getOrCreateBuffer((int) partitionIndex, (int) partitionSize);
        checkState(partition.limit() == partition.capacity());
        partition.position((int) partitionOffset);
        return partition;
    }

    public BigByteBuffer get(byte[] dst, int offset, int length) {
        final long p = position();
        get(p, dst, offset, length);
        checkState(p == position());
        position(p + length);
        return this;
    }

    public BigByteBuffer put(final byte b) {
        checkOverflow(1);
        final long p = position();
        put(p, b);
        position(p + 1);
        return this;
    }

    /**
     * Absolute single byte put method, doesn't change the current position
     * 
     */
    public BigByteBuffer put(final long offset, final byte b) {
        checkOverflow(offset, 1);
        ByteBuffer partition = getPartitionAt(offset);
        if (!partition.hasRemaining()) {
            partition = getPartitionAt(offset + 1);
        }
        partition.put(b);
        return this;
    }

    public final BigByteBuffer put(byte[] src) {
        return put(src, 0, src.length);
    }

    public BigByteBuffer put(final byte[] src, final int offset, final int length) {
        long p = position();
        put(p, src, offset, length);
        checkState(p == position());
        position(p + length);
        return this;
    }

    public BigByteBuffer put(final long offset, final byte[] src, final int srcOffset,
            final int length) {
        checkNotNull(src, "byte[] src is null");
        checkArgument(srcOffset < src.length && length <= src.length - srcOffset);
        checkOverflow(offset, length);

        int written = 0;
        while (written < length) {
            ByteBuffer buff = getPartitionAt(offset + written);
            int remaining = buff.remaining();
            int count = Math.min(length - written, remaining);
            buff.put(src, written, count);
            written += count;
        }
        return this;
    }

    /**
     * Absolute get to dst buffer, doesn't change this buffer's state.
     * 
     * @param offset
     * @param dst
     * @param dstOffset
     * @param length
     * @return
     */
    public BigByteBuffer get(final long offset, byte[] dst, int dstOffset, int length) {
        checkOverflow(offset, length);
        checkNotNull(dst, "byte[] dst is null");
        checkArgument(dstOffset < dst.length && length <= dst.length - dstOffset);

        int read = 0;
        while (read < length) {
            ByteBuffer buff = getPartitionAt(offset + read);
            int remaining = buff.remaining();
            int count = Math.min(length - read, remaining);
            buff.get(dst, read, count);
            read += count;
        }
        return this;
    }

    public short getShort() {
        final long p = position();
        final short i = getShort(p);
        checkState(p == position());
        position(p + Short.BYTES);
        return i;
    }

    public short getShort(final long offset) {
        ByteBuffer partition = getPartitionAt(offset);
        if (partition.remaining() > 1) {
            return partition.getShort();
        }
        // hit a partition edge
        byte[] buff = this.getTmpBuff();
        get(offset, buff, 0, Short.BYTES);
        short i = Shorts.fromByteArray(buff);
        return i;
    }

    public BigByteBuffer putShort(final short v) {
        final long p = position();
        putShort(p, v);
        checkState(p == position());
        position(p + Short.BYTES);
        return this;
    }

    public BigByteBuffer putShort(final long offset, final short v) {
        ByteBuffer partition = getPartitionAt(offset);
        if (partition.remaining() > 1) {
            partition.putShort(v);
            return this;
        }
        // hit a partition edge
        byte[] buff = this.getTmpBuff();
        buff[0] = (byte) ((v >>> 8) & 0xFF);
        buff[1] = (byte) ((v >>> 0) & 0xFF);
        put(offset, buff, 0, 2);
        return this;
    }

    public int getInt() {
        final long p = position();
        final int i = getInt(p);
        checkState(p == position());
        position(p + 4);
        return i;
    }

    public int getInt(final long offset) {
        ByteBuffer partition = getPartitionAt(offset);
        if (partition.remaining() > 3) {
            return partition.getInt();
        }
        // hit a partition edge
        byte[] buff = this.getTmpBuff();
        get(offset, buff, 0, 4);
        int i = Ints.fromByteArray(buff);
        return i;
    }

    /**
     * Creates a new byte buffer whose content is a shared subsequence of this buffer's content.
     *
     * <p>
     * The content of the new buffer will start at this buffer's current position. Changes to this
     * buffer's content will be visible in the new buffer, and vice versa; the two buffers'
     * position, limit, and mark values will be independent.
     *
     * <p>
     * The new buffer's position will be zero, its capacity and its limit will be the number of
     * bytes remaining in this buffer, and its mark will be undefined. The new buffer will be direct
     * if, and only if, this buffer is direct, and it will be read-only if, and only if, this buffer
     * is read-only.
     * </p>
     *
     * @return The new byte buffer
     */
    public BigByteBuffer slice() {
        return BigByteBuffer.slice(this);
    }

    static BigByteBuffer slice(BigByteBuffer buff) {
        long offset = buff.offset + buff.position();
        long pos = 0;
        long lim = buff.remaining();
        AtomicLong cap = new AtomicLong(lim);
        long mark = -1;
        return new BigByteBuffer(buff.store, buff.partitionSize, mark, pos, lim, cap,
                buff.growAutomatically, offset);
    }

    public BigByteBuffer duplicate() {
        return new BigByteBuffer(store, partitionSize, mark(), position(), limit(), super.capacity,
                super.growAutomatically, super.offset);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName()).append("[pos=")
                .append(position()).append(", lim=").append(limit()).append(", cap=")
                .append(capacity()).append(", slice offset:").append(offset).append(", autogrow: ")
                .append(growAutomatically).append(", partition: ").append(partitionSize)
                .append("]");
        return sb.toString();
    }

    private void checkOverflow(int count) throws java.nio.BufferOverflowException {
        if (remaining() < count) {
            if (growAutomatically) {
                grow(position() + count);
            } else {
                throw new BufferOverflowException(String.format("count: %d, %s", count, this));
            }
        }
    }

    private void checkOverflow(final long offset, final int count)
            throws java.nio.BufferOverflowException {
        if (capacity() - offset < count) {
            if (growAutomatically) {
                grow(offset + count);
            } else {
                throw new BufferOverflowException(String.format("at pos: %d, count: %d, %s",
                        offset, count, this));
            }
        }
    }

    private byte[] getTmpBuff() {
        Arrays.fill(tmpBuff, (byte) 0);
        return tmpBuff;
    }

    private static class BufferOverflowException extends java.nio.BufferOverflowException {

        private static final long serialVersionUID = 4672861429104392798L;

        private String msg;

        BufferOverflowException(String msg) {
            super();
            this.msg = msg;
        }

        @Override
        public String getMessage() {
            return msg;
        }
    }
}
