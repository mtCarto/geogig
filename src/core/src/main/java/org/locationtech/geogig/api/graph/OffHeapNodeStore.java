package org.locationtech.geogig.api.graph;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.DataInput;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.io.BigByteBuffer;
import org.locationtech.geogig.io.ByteBufferStore;
import org.locationtech.geogig.io.MappedFileByteBufferStore;

import com.google.common.base.Throwables;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

class OffHeapNodeStore {

    private final AtomicLong nextNodeOffset;

    private BigByteBuffer nodesBuffer;

    private NodeIdBigMap nodeOffsets;

    OffHeapNodeStore() throws IOException {
        final Path baseDir = Paths.get(System.getProperty("java.io.tmpdir"));

        final Path nodesFile = Files.createTempFile(baseDir, "SparseNodeStore", ".nodes");
        Files.delete(nodesFile);

        final Path indexFile = nodesFile.resolveSibling(nodesFile.getFileName().toString()
                .replace(".nodes", ".idx"));
        {
            ByteBufferStore store = new MappedFileByteBufferStore(indexFile, true);
            BigByteBuffer buffer = new BigByteBuffer(store, 128 * 1024 * 1024, true);
            nodeOffsets = new NodeIdBigMap(buffer);
        }

        final boolean nodeFileIsSparse = false;
        ByteBufferStore nodeStore = new MappedFileByteBufferStore(nodesFile, nodeFileIsSparse);
        nodesBuffer = new BigByteBuffer(nodeStore, 128 * 1024 * 1024, true);

        nextNodeOffset = new AtomicLong();
    }

    public void dispose() {
        if (nodesBuffer != null) {
            try {
                nodesBuffer.discard();
            } finally {
                try {
                    nodeOffsets.dispose();
                } finally {
                    nodesBuffer = null;
                    // indexBuffer = null;
                }
            }
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (nodesBuffer != null) {
            dispose();
        }
        super.finalize();
    }

    public DAGNode get(NodeId nodeId) {
        final long nodeOffset;
        nodeOffset = nodeOffsets.get(nodeId);
        checkArgument(nodeOffset > -1L);

        BigByteBuffer buff = nodesBuffer.duplicate();
        buff.position(nodeOffset);
        DataInput in = new BigMapDataInput(buff);

        DAGNode node;
        try {
            node = DAGNode.decode(in);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return node;
    }

    void put(NodeId nodeId, Node node) {
        put(nodeId, DAGNode.of(node));
    }

    public void put(NodeId nodeId, DAGNode node) {
        ByteArrayDataOutput data = ByteStreams.newDataOutput(64);
        try {
            DAGNode.encode(node, data);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        final byte[] encodedNode = data.toByteArray();
        final int nodeLength = encodedNode.length;

        final long nodeOffset = nextNodeOffset.getAndAdd(nodeLength);

        nodesBuffer.put(nodeOffset, encodedNode, 0, nodeLength);

        nodeOffsets.put(nodeId, nodeOffset);
    }

    private static class BigMapDataInput implements DataInput {

        private BigByteBuffer buffer;

        BigMapDataInput(BigByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public void readFully(byte[] b) throws IOException {
            readFully(b, 0, b.length);
        }

        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            try {
                buffer.get(b, off, len);
            } catch (BufferOverflowException e) {
                throw new IOException(e);
            }
        }

        @Override
        public int skipBytes(int n) throws IOException {
            int skipped = (int) Math.min(buffer.remaining(), n);
            buffer.position(buffer.position() + skipped);
            return skipped;
        }

        @Override
        public boolean readBoolean() throws IOException {
            byte b = buffer.get();
            return b != 0;
        }

        @Override
        public byte readByte() throws IOException {
            try {
                return buffer.get();
            } catch (BufferOverflowException e) {
                throw new IOException(e);
            }
        }

        @Override
        public int readUnsignedByte() throws IOException {
            try {
                return buffer.get() & 0xFF;
            } catch (BufferOverflowException e) {
                throw new IOException(e);
            }
        }

        @Override
        public short readShort() throws IOException {
            try {
                return buffer.getShort();
            } catch (BufferOverflowException e) {
                throw new IOException(e);
            }
        }

        @Override
        public int readUnsignedShort() throws IOException {
            try {
                return buffer.getShort() & 0xFFFF;
            } catch (BufferOverflowException e) {
                throw new IOException(e);
            }
        }

        @Override
        public char readChar() throws IOException {
            try {
                return (char) ((buffer.get() << 8) | (buffer.get() & 0xff));
            } catch (BufferOverflowException e) {
                throw new IOException(e);
            }
        }

        @Override
        public int readInt() throws IOException {
            try {
                return buffer.getInt();
            } catch (BufferOverflowException e) {
                throw new IOException(e);
            }
        }

        @Override
        public long readLong() throws IOException {
            try {
                return buffer.getLong();
            } catch (BufferOverflowException e) {
                throw new IOException(e);
            }
        }

        @Override
        public float readFloat() throws IOException {
            try {
                return buffer.getFloat();
            } catch (BufferOverflowException e) {
                throw new IOException(e);
            }
        }

        @Override
        public double readDouble() throws IOException {
            try {
                return buffer.getDouble();
            } catch (BufferOverflowException e) {
                throw new IOException(e);
            }
        }

        @Override
        public String readLine() throws IOException {
            return readUTF();
        }

        @Override
        public String readUTF() throws IOException {
            // read length using absolute offset to is doesn't change the buffer's position
            final int utflen = buffer.getShort(buffer.position());
            byte[] buff = new byte[2 + utflen];
            buffer.get(buff, 0, 2 + utflen);
            return ByteStreams.newDataInput(buff).readUTF();
        }

    }
}
