package org.locationtech.geogig.api.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.locationtech.geogig.api.plumbing.diff.RevObjectTestSupport.featureNode;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.storage.ObjectStore;

public class OffHeapNodeStoreTest {

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
    public void testNodeRoundtrip() {
        Node node = featureNode("some-feature-id-prefix", 0);
        NodeId nodeId = cstrategy.computeId(node);
        store.put(nodeId, node);

        DAGNode retrieved = store.get(nodeId);
        assertNotSame(node, retrieved.resolve(null));
        assertEquals(node, retrieved.resolve(null));
    }

    @Test
    public void testLazyFeatureNodeRoundtrip() {
        NodeId nodeId = cstrategy.computeId(featureNode("prefix", 0));
        DAGNode node = DAGNode.featureNode(9, 511);
        store.put(nodeId, node);

        DAGNode retrieved = store.get(nodeId);

        assertEquals(node, retrieved);
    }

    @Test
    public void testLazyTreeNodeRoundtrip() {
        NodeId nodeId = cstrategy.computeId(featureNode("prefix", 0));
        DAGNode node = DAGNode.treeNode(9, 511);
        store.put(nodeId, node);

        DAGNode retrieved = store.get(nodeId);

        assertEquals(node, retrieved);
    }

    @Test
    public void testSeveralLazyFeatureNodeRoundtrips() {
        final int count = 10_000;
        Map<NodeId, DAGNode> nodes = new HashMap<>();
        for (int i = 0; i < count; i++) {
            NodeId nodeId = cstrategy.computeId(featureNode("prefix", i));
            DAGNode node = DAGNode.featureNode(9, i);
            nodes.put(nodeId, node);
            store.put(nodeId, node);
            DAGNode retrieved = store.get(nodeId);
            assertEquals(node, retrieved);
        }

        List<NodeId> nodeIds = new ArrayList<NodeId>(nodes.keySet());
        Collections.shuffle(nodeIds);

        for (NodeId nodeId : nodeIds) {
            DAGNode node = nodes.get(nodeId);
            DAGNode retrieved = store.get(nodeId);
            assertEquals(node, retrieved);
        }
    }

    @Test
    public void testNonExistent() {
        Node node1 = featureNode("some-feature-id-prefix", 0);
        Node node2 = featureNode("some-feature-id-prefix", 1);

        NodeId nodeId1 = cstrategy.computeId(node1);
        NodeId nodeId2 = cstrategy.computeId(node2);

        store.put(nodeId1, node1);

        assertNotNull(store.get(nodeId1));
        ex.expect(IllegalArgumentException.class);
        store.get(nodeId2);
    }

    @Test
    public void testOverride() {
        Node node1 = featureNode("some-feature-id-prefix", 0, true);
        Node node2 = featureNode("some-feature-id-prefix", 0, false);
        assertEquals(node1.getName(), node2.getName());
        assertNotEquals(node1.getObjectId(), node2.getObjectId());

        NodeId nodeId = cstrategy.computeId(node1);
        assertEquals(nodeId, cstrategy.computeId(node2));

        store.put(nodeId, node1);
        assertEquals(node1, store.get(nodeId).resolve(null));

        store.put(nodeId, node2);
        assertEquals(node2, store.get(nodeId).resolve(null));
    }
}
