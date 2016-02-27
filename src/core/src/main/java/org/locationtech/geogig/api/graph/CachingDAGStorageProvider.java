package org.locationtech.geogig.api.graph;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.storage.ObjectStore;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

final class CachingDAGStorageProvider implements DAGStorageProvider {

    private static final int SWAP_THRESHOLD = 4096;

    private ObjectStore source;

    private TreeCache treeCache;

    private DAGStorageProvider delegate;

    private volatile boolean swapped = false;

    CachingDAGStorageProvider(ObjectStore source) {
        this.source = source;
        this.treeCache = new TreeCache(source);
        delegate = new HeapDAGStorageProvider(this.source, this.treeCache);
    }

    @Override
    public TreeCache getTreeCache() {
        return delegate.getTreeCache();
    }

    @Override
    public DAG getOrCreateTree(TreeId treeId) {
        return delegate.getOrCreateTree(treeId);
    }

    @Override
    public DAG getOrCreateTree(TreeId treeId, ObjectId originalTreeId) {
        return delegate.getOrCreateTree(treeId, originalTreeId);
    }

    @Override
    public Node getNode(NodeId nodeId) {
        return delegate.getNode(nodeId);
    }

    @Override
    public void saveNode(NodeId nodeId, Node node) {
        delegate.saveNode(nodeId, node);
        swap();
    }

    @Override
    public void saveNodes(Map<NodeId, DAGNode> nodeMappings) {
        delegate.saveNodes(nodeMappings);
        swap();
    }

    private void swap() {
        if (this.swapped) {
            return;
        }
        if (delegate.nodeCount() < SWAP_THRESHOLD) {
            return;
        }
        Preconditions.checkState(delegate instanceof HeapDAGStorageProvider);

        Map<NodeId, DAGNode> nodes = new TreeMap<>(((HeapDAGStorageProvider) delegate).nodes);
        Map<TreeId, DAG> trees = new TreeMap<>(((HeapDAGStorageProvider) delegate).trees);

        MappedFileDAGStorageProvider largeStore;
        try {
            largeStore = new MappedFileDAGStorageProvider(this.source, this.treeCache);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        try {
            largeStore.saveNodes(nodes);

            for (Map.Entry<TreeId, DAG> e : trees.entrySet()) {
                largeStore.save(e.getKey(), e.getValue());
            }
        } catch (RuntimeException e) {
            largeStore.dispose();
            throw e;
        }
        this.delegate = largeStore;
        this.swapped = true;
    }

    @Override
    public void dispose() {
        delegate.dispose();
    }

    @Override
    @Nullable
    public RevTree getTree(ObjectId originalId) {
        return delegate.getTree(originalId);
    }

    @Override
    public void save(TreeId bucketId, DAG bucketDAG) {
        delegate.save(bucketId, bucketDAG);
    }

    @Override
    public long nodeCount() {
        return delegate.nodeCount();
    }

}
