package org.locationtech.geogig.api.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runners.MethodSorters;
import org.locationtech.geogig.api.graph.NodeIdBigMap.DataPage;
import org.locationtech.geogig.api.graph.NodeIdBigMap.Entry;
import org.locationtech.geogig.io.BigByteBuffer;
import org.locationtech.geogig.io.ByteBufferStore;
import org.locationtech.geogig.io.HeapBufferStore;
import org.locationtech.geogig.io.MappedFileByteBufferStore;
import org.locationtech.geogig.storage.NodePathStorageOrder;

import com.google.common.base.Stopwatch;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NodeIdBigpMapStressTest {

    private NodeIdBigMap map;

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @After
    public void after() {
        if (map != null) {
            map.dispose();
        }
    }

    private NodeIdBigMap createOffHeapMap() {
        final Path indexFile = tmp.getRoot().toPath().resolve(testName.getMethodName() + ".index");
        ByteBufferStore store = new MappedFileByteBufferStore(indexFile, true);
        int partitionSize = 64 * 1024 * 1024;
        BigByteBuffer buffer = new BigByteBuffer(store, partitionSize, true);
        map = new NodeIdBigMap(buffer);
        return map;
    }

    private NodeIdBigMap createHeapMap() {
        ByteBufferStore store = new HeapBufferStore();
        final int partitionSize = 16 * 1024;// 16MB
        BigByteBuffer buffer = new BigByteBuffer(store, partitionSize, true);
        map = new NodeIdBigMap(buffer);
        return map;
    }

    @Test
    public void testHeap01_100K() {
        testPut(createHeapMap(), 100_000);
    }

    @Ignore
    @Test
    public void testHeap02_1M() {
        testPut(createHeapMap(), 1000_000);
    }

    @Ignore
    @Test
    public void testHeap03_10M() {
        testPut(createHeapMap(), 10_000_000);
    }

    @Test
    public void testOffHeap01_100K() {
        testPut(createOffHeapMap(), 100_000);
    }

    @Test
    public void testOffHeap02_1M() {
        testPut(createOffHeapMap(), 1000_000);
    }

    @Ignore
    @Test
    public void testOffHeap03_10M() {
        testPut(createOffHeapMap(), 10_000_000);
    }

    @Ignore
    @Test
    public void testOffHeap03_50M() {
        testPut(createOffHeapMap(), 50_000_000);
    }

    @Ignore
    @Test
    public void testOffHeap04_100M() {
        testPut(createOffHeapMap(), 100_000_000);
    }

    @Ignore
    @Test
    public void testOffHeap05_500M() {
        testPut(createOffHeapMap(), 500_000_000);
    }

    @Ignore
    @Test
    public void testOffHeap06_1000M() {
        testPut(createOffHeapMap(), 1000_000_000);
    }

    private void testPut(NodeIdBigMap map, final int count) {
        System.err.printf("---------------\n%s.%s(%,d).....\n", getClass().getSimpleName(),
                testName.getMethodName(), count);

        // for reporting with tabs, makes it easier to c&p on a spreadsheet
        final double insertTime, query10PercentTime, totalQueryTime;
        final long totalDataSpace, usedDataSpace, dataPages, bucketPages, totalBucketsSize;

        Stopwatch sw = Stopwatch.createStarted();
        for (int i = 0; i < count; i++) {
            long value = i * 64;
            NodeId id = id(i);

            map.put(id, value);
            if (i % (count / 100D) == 0) {
                System.err.print(".");
            }
            // Long val = map.get(id);
            // assertNotNull(val);
            // assertEquals(value, val.longValue());
        }
        sw.stop();
        insertTime = sw.elapsed(TimeUnit.MILLISECONDS);
        final long size = map.size();
        System.err.printf("\n%,d nodeIds inserted in %s\n", size, sw);
        dataPages = map.fileHeader.dataPages();
        totalDataSpace = map.fileHeader.dataPagesSize.get();
        usedDataSpace = dataPages * DataPage.HEADER_SIZE + size * Entry.SIZE_BYTES;
        int freeDataPages = map.fileHeader.freeDataPages();
        System.err.printf("Data pages: %,d, free: %,d, Total size: %,d, Used size: %,d\n",
                dataPages, freeDataPages, totalDataSpace, usedDataSpace);

        bucketPages = map.fileHeader.bucketPages();
        totalBucketsSize = map.fileHeader.bucketPagesSize.get();
        System.err.printf("Bucket pages: %,d, Total size: %,d\n", bucketPages, totalBucketsSize);
        assertEquals(count, size);
        assertTrue(map.get(id(7000)) > -1L);
        // / assertEquals(count, map.sizeRecursive());

        final int queryCount = count / 10;

        System.err.printf("Running %,d nodeId queries...\n", queryCount);

        sw.reset().start();
        Random rnd = new Random();
        for (int i = 0; i < queryCount; i++) {
            NodeId nodeId = id(rnd.nextInt(count));
            long value = map.get(nodeId);
            if (value == -1L) {
                fail("value for " + nodeId + " not found");
            }
        }
        sw.stop();
        query10PercentTime = sw.elapsed(TimeUnit.MILLISECONDS);
        System.err.printf("%,d random nodeIds queried in %s\n", queryCount, sw);

        sw.reset().start();
        for (int i = 0; i < count; i++) {
            NodeId id = id(i);
            long value = map.get(id);
            if (value == -1L) {
                fail("value for " + id + " not found");
            }
        }
        sw.stop();
        totalQueryTime = sw.elapsed(TimeUnit.MILLISECONDS);
        System.err.printf("%,d nodeIds queried in %s\n", count, sw);

        System.err.printf(
                "Time (insert, query 10 percent, query 100 percent):\n%.4f\t%.4f\t%.4f\n",
                insertTime, query10PercentTime, totalQueryTime);
        System.err
                .printf("Size (data total, data used, data pages, bucket pages, buckets size):\n%,d\t%,d\t%,d\t%,d\t%,d\n",
                        totalDataSpace, usedDataSpace, dataPages, bucketPages, totalBucketsSize);

    }

    private NodeId id(int i) {
        String name = "feature." + i;
        return new CanonicalNodeId(NodePathStorageOrder.INSTANCE.hashCodeLong(name), name);
    }

}
