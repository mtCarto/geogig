package org.locationtech.geogig.api.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.locationtech.geogig.api.graph.NodeIdBigMap.Entry;
import org.locationtech.geogig.api.graph.NodeIdBigMap.FileHeader;
import org.locationtech.geogig.io.BigByteBuffer;
import org.locationtech.geogig.io.ByteBufferStore;
import org.locationtech.geogig.io.HeapBufferStore;
import org.locationtech.geogig.storage.NodePathStorageOrder;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;

public class NodeIdBigpMapTest {

    private BigByteBuffer buffer;

    @Rule
    public ExpectedException ex = ExpectedException.none();

    @Before
    public void before() {
        ByteBufferStore store = new HeapBufferStore();
        int partitionSize = 8 * 1024;
        boolean growAutomatically = true;
        buffer = new BigByteBuffer(store, partitionSize, growAutomatically);
    }

    @After
    public void after() {
        if (buffer != null) {
            buffer.discard();
        }
    }

    @Test
    public void fileHeaderCreate() {
        FileHeader header = FileHeader.create(buffer);
        assertSame(buffer, header.dataBuffer);
    }

    @Test
    public void testDispose(){
        buffer = spy(buffer);
        NodeIdBigMap map = new NodeIdBigMap(buffer);
        map.dispose();
        verify(buffer, times(1)).discard();
        assertNull(map.dataBuffer);
        assertNull(map.fileHeader);
    }

    @Test
    public void entryEncoding() {
        final long bucketHash = 0xFFFFCCCL;
        final byte nameHash = (byte) 0xAB;
        final Entry entry = new Entry(bucketHash, nameHash);

        assertNotNull(entry.encodedKey);
        assertEquals(Entry.KEY_SIZE, entry.encodedKey.length);

        ByteArrayDataInput in = ByteStreams.newDataInput(entry.encodedKey);
        assertEquals(bucketHash, in.readLong());
        assertEquals(nameHash, in.readByte());
    }

    @Test
    public void testPut_100() {
        testPut(100);
    }

    @Test
    public void testPut_1K() {
        testPut(1000);
    }

    @Test
    public void testPut_100K() {
        testPut(100_000);
    }

    @Test
    public void testPut_1M() {
        testPut(1000_000);
    }

    private void testPut(final int count) {
        NodeIdBigMap map = new NodeIdBigMap(buffer);
        // put
        for (int i = 0; i < count; i++) {
            int insertionIndex = map.put(id("node-" + i), i * 1000);
            // insertionIndex < 1 means it was new
            assertTrue("" + insertionIndex, insertionIndex < 0);
        }

        assertEquals(count, map.size());
        assertEquals(count, map.sizeRecursive());
        
        for (int i = 0; i < count; i++) {
            NodeId id = id("node-" + i);
            long value = map.get(id);
            assertEquals(i * 1000, value);
        }

        // override
        for (int i = 0; i < count; i++) {
            int insertionIndex = map.put(id("node-" + i), i * 10_000);
            assertFalse("" + insertionIndex, insertionIndex < 0);
        }

        assertEquals(count, map.size());
        assertEquals(count, map.sizeRecursive());

        for (int i = 0; i < count; i++) {
            NodeId id = id("node-" + i);
            long value = map.get(id);
            assertEquals(i * 10_000, value);
        }

        // random query
        Random rnd = new Random();
        for (int i = 0; i < count; i++) {
            int c = rnd.nextInt(count);
            NodeId id = id("node-" + c);
            long value = map.get(id);
            assertEquals(c * 10_000, value);
        }
    }

    private NodeId id(String name) {
        return new CanonicalNodeId(NodePathStorageOrder.INSTANCE.hashCodeLong(name), name);
    }

}
