package org.locationtech.geogig.io;

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.InvalidMarkException;
import java.util.concurrent.atomic.AtomicLong;

public class BigBuffer {

    /**
     * Absolute offset where this buffer starts
     */
    protected final long offset;

    private long limit, position, mark = -1/* unset */;

    protected final AtomicLong capacity;

    protected final boolean growAutomatically;

    protected final int partitionSize;

    BigBuffer(long mark, long pos, long lim, AtomicLong cap, boolean growAutomatically,
            int partitionSize, long offset) {
        this.mark = mark;
        this.position = pos;
        this.limit = lim;
        this.capacity = cap;
        this.growAutomatically = growAutomatically;
        this.partitionSize = partitionSize;
        this.offset = offset;
    }

    public final long limit() {
        return this.limit;
    }

    public final long capacity() {
        return this.capacity.longValue();
    }

    public final long mark() {
        return this.mark;
    }

    public final long position() {
        return this.position;
    }

    public final long remaining() {
        return limit - position;
    }

    public final boolean hasRemaining() {
        return this.position < this.limit;
    }

    /**
     * Resets this buffer's position to the previously-marked position.
     *
     * <p>
     * This method doesn't change or discard the mark
     * 
     * @return {@code this}
     * @throws InvalidMarkException If the mark has not been set
     */
    public final void reset() {
        final long mark = this.mark;
        if (mark == -1) {
            throw new InvalidMarkException();
        }
        position(mark);
    }

    /**
     * Clears this buffer. The position is set to zero, the limit is set to the capacity, and the
     * mark is discarded.
     */
    public final void clear() {
        this.limit = this.capacity.longValue();
        this.position = 0L;
        this.mark = -1L;
    }

    /**
     * Flips this buffer. The limit is set to the current position and then the position is set to
     * zero. If the mark is defined then it is discarded.
     */
    public final void flip() {
        this.limit = this.position;
        this.position = 0;
        this.mark = -1;
    }

    /**
     * Rewinds this buffer. The position is set to zero and the mark is discarded.
     */
    public final void rewind() {
        this.position = 0;
        this.mark = -1;
    }

    /**
     * Sets this buffer's limit. If the position is larger than the new limit then it is set to the
     * new limit. If the mark is defined and larger than the new limit then it is discarded.
     *
     * @param limit The new limit value; must be non-negative and no larger than this buffer's
     *        capacity
     *
     * @return This buffer
     *
     * @throws IllegalArgumentException if the new limit is outside the buffer boundaries
     */
    public final BigBuffer limit(final long limit) {
        checkArgument(limit >= 0, "limit must be >= 0");
        if (limit > capacity()) {
            if (growAutomatically) {
                grow(limit);
            } else {
                throw new IllegalArgumentException(String.format("Invalid limit: %,d. %s", limit,
                        this));
            }
        }
        this.limit = limit;
        if (mark > limit) {
            mark = -1;
        }
        if (position > limit) {
            position = limit;
        }
        return this;
    }

    protected void grow(final long minCapacity) {
        final long currentCapacity = capacity();
        final long newCapacity = partitionSize * (1 + minCapacity / partitionSize);
        if (newCapacity > currentCapacity) {
            this.capacity.set(newCapacity);
            limit(capacity());
        }
    }

    public final void position(final long position) {
        checkArgument(position > -1 && position <= limit);

        this.position = position;
        if (mark > position) {
            mark = -1;
        }
    }

}
