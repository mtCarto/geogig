package org.locationtech.geogig.api.graph;

import static org.locationtech.geogig.api.plumbing.diff.RevObjectTestSupport.featureNode;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;
import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.storage.ObjectStore;

import com.google.common.base.Stopwatch;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OffHeapNodeStorePerformanceTest {

    private OffHeapNodeStore store;

    private ClusteringStrategy cstrategy;

    @Rule
    public ExpectedException ex = ExpectedException.none();

    @Before
    public void before() throws IOException {
        store = new OffHeapNodeStore();
        DAGStorageProviderFactory dagStorageFactory = new HeapDAGStorageProviderFactory(
                mock(ObjectStore.class));
        cstrategy = ClusteringStrategyFactory.canonical().create(RevTree.EMPTY, dagStorageFactory);
    }

    @After
    public void after() {
        if (store != null) {
            store.dispose();
        }
    }

    @Test
    public void loadTest01_100K() throws Exception {
        testLoad(100_000);
    }

    @Test
    public void loadTest02_1M() throws Exception {
        testLoad(1_000_000);
    }

    @Ignore
    @Test
    public void loadTest03_5M() throws Exception {
        testLoad(5_000_000);
    }

    @Ignore
    @Test
    public void loadTest04_10M() throws Exception {
        testLoad(10_000_000);
    }

    private void testLoad(final int count) throws Exception {
        Stopwatch sw = Stopwatch.createStarted();
        for (int i = 0; i < count; i++) {
            try {
                Node node = featureNode("large-feature-id-prefix-", i, true);
                NodeId nodeId = cstrategy.computeId(node);
                store.put(nodeId, node);
            } catch (Exception e) {
                System.err.println("at " + i);
                e.printStackTrace();
                throw e;
            }
        }
        sw.stop();
        System.err.printf("%,d nodes added in %s\n", count, sw);

        sw = Stopwatch.createStarted();
        for (int i = 0; i < count / 10; i++) {
            try {
                Node node = featureNode("large-feature-id-prefix-", i, true);
                NodeId nodeId = cstrategy.computeId(node);
                store.get(nodeId);
            } catch (Exception e) {
                System.err.println("at " + i);
                e.printStackTrace();
                throw e;
            }
        }
        sw.stop();
        System.err.printf("%,d nodes queried in %s\n", count / 10, sw);
    }
}
