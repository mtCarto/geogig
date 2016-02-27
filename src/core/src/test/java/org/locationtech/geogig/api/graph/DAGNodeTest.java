package org.locationtech.geogig.api.graph;

import static org.junit.Assert.*;
import static org.locationtech.geogig.api.plumbing.diff.RevObjectTestSupport.createFeaturesTree;
import static org.locationtech.geogig.api.plumbing.diff.RevObjectTestSupport.createTreesTree;

import java.io.DataOutput;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.api.graph.DAGNode.FeatureDAGNode;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.storage.memory.HeapObjectStore;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import static org.mockito.Mockito.*;

public class DAGNodeTest {

    private TreeCache cache;

    private RevTree featuresTree;

    private RevTree treesTree;

    @Before
    public void before() {
        ObjectStore store = new HeapObjectStore();
        store.open();
        cache = mock(TreeCache.class);
        featuresTree = createFeaturesTree(store, "f", 512);
        treesTree = createTreesTree(store, 2, 200, ObjectId.forString("test"));
    }

    @Test
    public void lazyFeatureNodeCreate() {
        DAGNode node = DAGNode.featureNode(5, 511);
        assertTrue(node instanceof FeatureDAGNode);
        FeatureDAGNode fnode = (FeatureDAGNode) node;
        assertEquals(5, fnode.leafRevTreeId);
        assertEquals(511, fnode.nodeIndex);
        assertFalse("a lazy feature node can never be nil", node.isNull());
    }

    @Test
    public void lazyFeatureNodeEquals() {
        DAGNode node = DAGNode.featureNode(5, 511);
        assertEquals(node, DAGNode.featureNode(5, 511));
        assertNotEquals(node, DAGNode.featureNode(5, 510));
        assertNotEquals(node, DAGNode.featureNode(4, 511));
    }

    @Test
    public void lazyFeatureNodeResolve() {
        DAGNode node = DAGNode.featureNode(5, 511);

        when(cache.resolve(eq(5))).thenReturn(featuresTree);
        Node resolved = node.resolve(cache);
        assertNotNull(resolved);
        assertSame(featuresTree.features().get().get(511), resolved);
    }

    @Test
    public void lazyFeatureNodeEncodeDecode() throws IOException {
        DAGNode node = DAGNode.featureNode(5, 511);
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        DAGNode.encode(node, out);

        DAGNode decoded = DAGNode.decode(ByteStreams.newDataInput(out.toByteArray()));
        assertEquals(node, decoded);
    }
}
