package org.locationtech.geogig.io;

import java.nio.ByteBuffer;

import org.eclipse.jdt.annotation.Nullable;

public interface ByteBufferStore {

    public @Nullable ByteBuffer getIfInitialized(int identifier);

    public ByteBuffer getOrCreateBuffer(int identifier, int size);

    public void discard();

}
