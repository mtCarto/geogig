package org.locationtech.geogig.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import org.eclipse.jdt.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class MappedFileByteBufferStore implements ByteBufferStore {

    private static final Logger LOG = LoggerFactory.getLogger(MappedFileByteBufferStore.class);

    private ByteBuffer[] buffers = new ByteBuffer[10];

    private final Path file;

    private final FileChannel fileChannel;

    private final MapMode mapMode;

    public MappedFileByteBufferStore(final Path file, final boolean sparse) {
        this.file = file;

        try {
            Preconditions.checkState(!Files.exists(file));
            this.fileChannel = FileChannel.open(file, StandardOpenOption.SPARSE,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            Preconditions.checkState(Files.exists(file));
            this.mapMode = MapMode.READ_WRITE;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void discard() {
        try {
            Arrays.asList(buffers).forEach((b) -> clean(b));
            buffers = null;
        } finally {
            try {
                fileChannel.close();
                Files.deleteIfExists(file);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private void clean(@Nullable ByteBuffer b) {
        if (b != null) {
            LOG.debug(String.format("cleaning buffer %s", b));
            IOUtil.clean(b);
        }
    }

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
            final long position = (long) index * size;
            try {
                buffer = fileChannel.map(mapMode, position, size);
                LOG.debug(String.format("created buffer %s at offset %,d.", buffer, position));
            } catch (IOException e) {
                discard();
                throw Throwables.propagate(e);
            }
            ensureCapacity(index);
            buffers[index] = buffer;
        }
        return buffer;
    }

    private void ensureCapacity(final int index) {
        if (buffers.length <= index) {
            int newSize = 10 * (1 + (index / 10));
            ByteBuffer[] tmp = new ByteBuffer[newSize];
            System.arraycopy(buffers, 0, tmp, 0, buffers.length);
            this.buffers = tmp;
        }
    }
}
