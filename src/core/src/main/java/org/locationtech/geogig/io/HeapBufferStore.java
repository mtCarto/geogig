package org.locationtech.geogig.io;

import java.nio.ByteBuffer;

import org.eclipse.jdt.annotation.Nullable;

public class HeapBufferStore implements ByteBufferStore {

    private ByteBuffer[] buffers = new ByteBuffer[10];

    @Override
    public @Nullable ByteBuffer getIfInitialized(final int index) {
        ByteBuffer buffer = null;
        if (index < buffers.length) {
            buffer = buffers[index];
        }
        return buffer;
    }

    @Override
    public ByteBuffer getOrCreateBuffer(final int index, final int size) {
        ByteBuffer buffer = getIfInitialized(index);
        if (null == buffer) {
            buffer = ByteBuffer.allocate(size);
            ensureCapacity(index);
            buffers[index] = buffer;
        }
        return buffer;
    }

    private void ensureCapacity(final int arrayIndex) {
        if (buffers.length <= arrayIndex) {
            ByteBuffer[] tmp = buffers;
            int newSize = 10 * (1 + arrayIndex / 10);
            ByteBuffer[] newarray = new ByteBuffer[newSize];
            System.arraycopy(tmp, 0, newarray, 0, tmp.length);
            this.buffers = newarray;
        }
    }

    @Override
    public void discard() {
        buffers = null;
    }

}
