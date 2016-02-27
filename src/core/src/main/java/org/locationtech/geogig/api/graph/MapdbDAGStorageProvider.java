/* Copyright (c) 2015 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.api.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.api.graph.DAG.STATE;
import org.locationtech.geogig.index.quadtree.QuadTreeNodeId;
import org.locationtech.geogig.index.quadtree.Quadrant;
import org.locationtech.geogig.storage.FieldType;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.storage.datastream.DataStreamValueSerializerV2;
import org.locationtech.geogig.storage.datastream.Varint;
import org.locationtech.geogig.storage.datastream.Varints;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.io.Closeables;
import com.google.common.primitives.UnsignedLong;

public class MapdbDAGStorageProvider implements DAGStorageProvider {

    private ObjectStore treeStore;

    private ConcurrentMap<NodeId, DAGNode> nodes;

    private ConcurrentMap<TreeId, DAG> trees;

    private TreeCache treeCache;

    private DB nodesDb;

    private DB treesDb;

    public static MapdbDAGStorageProvider canonical(ObjectStore treeStore) {

        return new MapdbDAGStorageProvider(treeStore, NodeIdBtreeSerializer.CANONICAL,
                DAGSerializer.CANONICAL);
    }

    public static MapdbDAGStorageProvider quadTree(ObjectStore treeStore) {
        return new MapdbDAGStorageProvider(treeStore, NodeIdBtreeSerializer.QUADTREE,
                DAGSerializer.QUADTREE);
    }

    private MapdbDAGStorageProvider(final ObjectStore treeStore,
            final BTreeKeySerializer<NodeId, List<NodeId>> nodeIdBtreeSerializer,
            final Serializer<DAG> dagSerializer) {

        final File nodesTmpFile;
        final File treesTmpFile;

        try {
            final File baseDir = new File(System.getProperty("java.io.tmpdir"));
            nodesTmpFile = File.createTempFile("mapdb-tmpNodes", ".db", baseDir);
            treesTmpFile = File.createTempFile("mapdb-tmpTrees", ".db", baseDir);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        this.treeStore = treeStore;
        nodesDb = DBMaker//
                .fileDB(nodesTmpFile)//
                .fileLockDisable()//
                .fileChannelEnable()//
                // .compressionEnable()//
                .deleteFilesAfterClose()//
                .closeOnJvmShutdown()//
                .transactionDisable()//
                .asyncWriteEnable()//
                .asyncWriteFlushDelay(20_000)//
                // .executorEnable()//
                // .storeExecutorEnable()//
                // .cacheLRUEnable()//
                .cacheHashTableEnable()//
                .cacheSize(1_000_000)//
                .lockSingleEnable()//
                // .lockDisable()
                .allocateStartSize(0L)//
                .allocateIncrement(512 * 1024)//
                .make();

        // nodes = nodesDb.hashMap("nodes", CANONICAL_NODEID_SERIALIZER, NODE_SERIALIZER);
        // nodes = nodesDb.treeMap("nodes", CANONICAL_NODEID_SERIALIZER, NODE_SERIALIZER);

        nodes = nodesDb.treeMapCreate("nodes")//
                .keySerializer(nodeIdBtreeSerializer)//
                .valueSerializer(NODE_SERIALIZER)//
                .nodeSize(16)//
                .valuesOutsideNodesEnable().make();

        // nodes = new ConcurrentHashMap<>();

        treesDb = DBMaker//
                .fileDB(treesTmpFile)//
                .fileLockDisable()//
                .fileMmapEnableIfSupported()//
                .fileMmapCleanerHackEnable()//
                .fileChannelEnable()//
                .deleteFilesAfterClose()//
                .closeOnJvmShutdown()//
                .transactionDisable()//
                .asyncWriteEnable()//
                .asyncWriteFlushDelay(20_000)//
                // .executorEnable()//
                // .storeExecutorEnable()//
                .cacheLRUEnable()//
                // .cacheHashTableEnable()// breaks things if map is a hashmap
                .cacheSize(5_000_000)//
                // /.lockSingleEnable()//
                .allocateStartSize(0)//
                .allocateIncrement(2 * 1024 * 1024)//
                .make();

        trees = treesDb.hashMap("trees", TREEID_SERIALIZER, dagSerializer);

        // trees = new ConcurrentHashMap<>();

        // trees = treesDb.treeMap("trees", TREEID_SERIALIZER, dagSerializer);

        treeCache = new TreeCache(treeStore);
    }

    @Override
    public void dispose() {
        try {
            Closeables.close(nodesDb, true);
            Closeables.close(treesDb, true);
        } catch (IOException cantHappen) {
            throw new IOError(cantHappen);
        }
    }

    private DAG createTree(TreeId treeId, ObjectId originalTreeId) {
        DAG dag = new DAG(originalTreeId);
        DAG existing = trees.putIfAbsent(treeId, dag);
        Preconditions.checkState(existing == null, "DAG %s[%s] already exists: %s", treeId,
                originalTreeId, existing);
        return dag;
    }

    @Override
    public DAG getOrCreateTree(TreeId treeId) {
        return getOrCreateTree(treeId, RevTree.EMPTY_TREE_ID);
    }

    @Override
    public DAG getOrCreateTree(TreeId treeId, ObjectId originalTreeId) {
        DAG dag = trees.get(treeId);
        if (dag == null) {
            dag = createTree(treeId, originalTreeId);
        }
        return dag;
    }

    @Override
    public Node getNode(final NodeId nodeId) {
        DAGNode dagNode = nodes.get(nodeId);
        return dagNode == null ? null : dagNode.resolve(treeCache);
    }

    @Override
    public void saveNode(final NodeId nodeId, final Node node) {
        nodes.put(nodeId, DAGNode.of(node));
    }

    @Override
    public void saveNodes(Map<NodeId, DAGNode> nodeMappings) {
        nodes.putAll(nodeMappings);
    };

    @Override
    public RevTree getTree(ObjectId treeId) {
        return treeStore.getTree(treeId);
    }

    private static class NodeIdBtreeSerializer extends BTreeKeySerializer<NodeId, List<NodeId>> {

        public static final NodeIdBtreeSerializer CANONICAL = new NodeIdBtreeSerializer(
                CanonicalNodeIdsSerializer.LIST);

        public static final NodeIdBtreeSerializer QUADTREE = new NodeIdBtreeSerializer(
                QuadTreeNodeIdsSerializer.LIST);

        private final Serializer<List<NodeId>> nodeIdListSerializer;

        private NodeIdBtreeSerializer(Serializer<List<NodeId>> nodeIdListSerializer) {
            this.nodeIdListSerializer = nodeIdListSerializer;
        }

        @Override
        public List<NodeId> arrayToKeys(Object[] keys) {
            NodeId[] copy = new NodeId[keys.length];
            System.arraycopy(keys, 0, copy, 0, keys.length);
            return Lists.newArrayList(copy);
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public void serialize(DataOutput out, List<NodeId> keys) throws IOException {
            nodeIdListSerializer.serialize(out, keys);
        }

        @Override
        public List<NodeId> deserialize(DataInput in, int nodeSize) throws IOException {
            return nodeIdListSerializer.deserialize(in, -1);
        }

        @Override
        public int compare(List<NodeId> keys, int pos1, int pos2) {
            return keys.get(pos1).compareTo(keys.get(pos2));
        }

        @Override
        public int compare(List<NodeId> keys, int pos, NodeId key) {
            return keys.get(pos).compareTo(key);
        }

        @Override
        public NodeId getKey(List<NodeId> keys, int pos) {
            return keys.get(pos);
        }

        @Override
        public Comparator<NodeId> comparator() {
            return Ordering.natural();
        }

        @Override
        public List<NodeId> emptyKeys() {
            return new ArrayList<>(4);
        }

        @Override
        public int length(List<NodeId> keys) {
            return keys.size();
        }

        @Override
        public List<NodeId> putKey(List<NodeId> keys, int pos, NodeId newKey) {

            int insertionIndex = Collections.binarySearch(keys, newKey);
            if (insertionIndex < 0) {
                insertionIndex = Math.abs(insertionIndex) - 1;
            }
            keys.add(insertionIndex, newKey);
            return keys;
        }

        @Override
        public List<NodeId> copyOfRange(List<NodeId> keys, int from, int to) {
            List<NodeId> list = new ArrayList<NodeId>(1 + (to - from));
            list.addAll(keys.subList(from, to));
            return list;
        }

        @Override
        public List<NodeId> deleteKey(List<NodeId> keys, int pos) {
            keys.remove(pos);
            return keys;
        }
    };

    private static abstract class QuadTreeNodeIdsSerializer<T extends Collection<NodeId>> extends
            Serializer<T> {

        public static final Serializer<SortedSet<NodeId>> SET = new QuadTreeNodeIdsSerializer<SortedSet<NodeId>>() {
            @Override
            protected SortedSet<NodeId> createNew() {
                return new TreeSet<>();
            }
        };

        public static final Serializer<List<NodeId>> LIST = new QuadTreeNodeIdsSerializer<List<NodeId>>() {
            @Override
            protected List<NodeId> createNew() {
                return new ArrayList<>(4);
            }
        };

        @Override
        public void serialize(DataOutput out, @Nullable T value) throws IOException {

            final int size = value == null ? 0 : value.size();
            Varint.writeUnsignedVarInt(size, out);
            if (size > 0) {
                byte[][] quadrants = new byte[size][];
                String[] names = new String[size];
                int i = 0;
                for (NodeId nodeId : value) {
                    names[i] = nodeId.name;
                    Quadrant[] quadrantsByDepth = ((QuadTreeNodeId) nodeId).quadrantsByDepth();
                    final int depth = quadrantsByDepth.length;
                    quadrants[i] = new byte[depth];
                    for (int d = 0; d < depth; d++) {
                        quadrants[i][d] = (byte) quadrantsByDepth[d].ordinal();
                    }
                    i++;
                }

                DataStreamValueSerializerV2.write(names, out);
                for (i = 0; i < size; i++) {
                    Serializer.BYTE_ARRAY.serialize(out, quadrants[i]);
                }
            }
        }

        @Override
        public @Nullable T deserialize(DataInput in, int available) throws IOException {

            final int size = Varint.readUnsignedVarInt(in);
            T ids = null;
            if (size > 0) {
                ids = createNew();
                final String names[];
                names = (String[]) DataStreamValueSerializerV2.read(FieldType.STRING_ARRAY, in);
                for (int i = 0; i < size; i++) {
                    byte[] quadrantsByDepth = Serializer.BYTE_ARRAY.deserialize(in, -1);
                    QuadTreeNodeId nodeId = new QuadTreeNodeId(names[i], quadrantsByDepth);
                    ids.add(nodeId);
                }
            }
            return ids;
        }

        protected abstract T createNew();

    }

    private static Serializer<NodeId> CANONICAL_NODEID_SERIALIZER = new Serializer<NodeId>() {

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public void serialize(DataOutput out, NodeId value) throws IOException {
            CanonicalNodeId nid = (CanonicalNodeId) value;
            out.writeUTF(nid.name);
            Varint.writeSignedVarLong(nid.bucketsByDepth().longValue(), out);
        }

        @Override
        public NodeId deserialize(DataInput in, int available) throws IOException {
            String name = in.readUTF();
            long longVal = Varint.readSignedVarLong(in);

            return new CanonicalNodeId(UnsignedLong.fromLongBits(longVal), name);
        }
    };

    private static abstract class CanonicalNodeIdsSerializer<T extends Collection<NodeId>> extends
            Serializer<T> {

        public static final Serializer<SortedSet<NodeId>> SET = new CanonicalNodeIdsSerializer<SortedSet<NodeId>>() {
            @Override
            protected SortedSet<NodeId> createNew() {
                return new TreeSet<>();
            }
        };

        public static final Serializer<List<NodeId>> LIST = new CanonicalNodeIdsSerializer<List<NodeId>>() {
            @Override
            protected List<NodeId> createNew() {
                return new ArrayList<>(4);
            }
        };

        @Override
        public void serialize(DataOutput out, @Nullable T value) throws IOException {

            final int size = value == null ? 0 : value.size();
            Varint.writeUnsignedVarInt(size, out);
            if (size > 0) {
                long[] values = new long[size];
                String[] names = new String[size];
                int i = 0;
                for (NodeId nodeId : value) {
                    values[i] = ((CanonicalNodeId) nodeId).bucketsByDepth().longValue();
                    names[i] = nodeId.name;
                    i++;
                }
                int offset = 0;
                Varints.writeLongArrayDeltaEncoded(out, values, offset, size);
                DataStreamValueSerializerV2.write(names, out);
            }
        }

        @Override
        public @Nullable T deserialize(DataInput in, int available) throws IOException {

            final int size = Varint.readUnsignedVarInt(in);
            T ids = null;
            if (size > 0) {
                ids = createNew();
                long[] values = new long[size];
                String names[];
                Varints.readLongArrayDeltaEncoded(in, values, 0, size);
                names = (String[]) DataStreamValueSerializerV2.read(FieldType.STRING_ARRAY, in);
                for (int i = 0; i < size; i++) {
                    CanonicalNodeId nodeId = new CanonicalNodeId(
                            UnsignedLong.fromLongBits(values[i]), names[i]);
                    ids.add(nodeId);
                }
            }
            return ids;
        }

        protected abstract T createNew();
    };

    private static final Serializer<DAGNode> NODE_SERIALIZER = new Serializer<DAGNode>() {

        @Override
        public void serialize(DataOutput out, DAGNode value) throws IOException {
            DAGNode.encode(value, out);
        }

        @Override
        public DAGNode deserialize(DataInput in, int available) throws IOException {
            DAGNode node = DAGNode.decode(in);
            return node;
        }
    };

    private static final Serializer<TreeId> TREEID_SERIALIZER = new Serializer<TreeId>() {

        @Override
        public void serialize(DataOutput out, TreeId value) throws IOException {
            Serializer.BYTE_ARRAY.serialize(out, value.bucketIndicesByDepth);
        }

        @Override
        public TreeId deserialize(DataInput in, int available) throws IOException {
            byte[] rawId = Serializer.BYTE_ARRAY.deserialize(in, available);
            return new TreeId(rawId);
        }
    };

    private static final Serializer<SortedSet<TreeId>> TREEIDSET_SERIALIZER = new Serializer<SortedSet<TreeId>>() {

        @Override
        public void serialize(DataOutput out, @Nullable SortedSet<TreeId> value) throws IOException {
            final int size = value == null ? 0 : value.size();
            Varint.writeUnsignedVarInt(size, out);
            if (size > 0) {
                for (TreeId tid : value) {
                    Serializer.BYTE_ARRAY.serialize(out, tid.bucketIndicesByDepth);
                }
            }
        }

        @Override
        public @Nullable SortedSet<TreeId> deserialize(DataInput in, int available)
                throws IOException {
            final int size = Varint.readUnsignedVarInt(in);
            SortedSet<TreeId> set = null;
            if (size > 0) {
                set = new TreeSet<>();
                for (int i = 0; i < size; i++) {
                    set.add(new TreeId(Serializer.BYTE_ARRAY.deserialize(in, -1)));
                }
            }
            return set;
        }
    };

    static final class DAGSerializer extends Serializer<DAG> {

        public static final DAGSerializer CANONICAL = new DAGSerializer(
                CanonicalNodeIdsSerializer.SET);

        public static final DAGSerializer QUADTREE = new DAGSerializer(
                QuadTreeNodeIdsSerializer.SET);

        private final Serializer<SortedSet<NodeId>> nodeIdSetSerializer;

        DAGSerializer(Serializer<SortedSet<NodeId>> nodeIdSetSerializer) {
            this.nodeIdSetSerializer = nodeIdSetSerializer;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public void serialize(DataOutput out, DAG value) throws IOException {
            final ObjectId treeId = value.treeId;
            out.write(treeId.getRawValue());
            if (!treeId.equals(RevTree.EMPTY_TREE_ID)) {
                long childCount = value.childCount;
                STATE state = value.getState();

                SortedSet<NodeId> children = value.children();
                SortedSet<NodeId> unpromotable = value.unpromotable();
                SortedSet<TreeId> buckets = value.buckets();

                Varint.writeUnsignedVarLong(childCount, out);
                out.writeByte(state.ordinal());

                nodeIdSetSerializer.serialize(out, children);
                nodeIdSetSerializer.serialize(out, unpromotable);
                TREEIDSET_SERIALIZER.serialize(out, buckets);
            }
        }

        @Override
        public DAG deserialize(DataInput in, int available) throws IOException {
            ObjectId oid;
            {
                byte[] rawId = new byte[ObjectId.NUM_BYTES];
                in.readFully(rawId);
                oid = ObjectId.createNoClone(rawId);
            }

            long childCount = 0L;
            STATE state = STATE.INITIALIZED;
            SortedSet<NodeId> children = null;
            SortedSet<NodeId> unpromotable = null;
            SortedSet<TreeId> buckets = null;

            if (oid.equals(RevTree.EMPTY_TREE_ID)) {
                oid = RevTree.EMPTY_TREE_ID;
            } else {
                childCount = Varint.readUnsignedVarLong(in);
                state = STATE.values()[in.readByte() & 0xFF];

                children = nodeIdSetSerializer.deserialize(in, available);
                unpromotable = nodeIdSetSerializer.deserialize(in, available);
                buckets = TREEIDSET_SERIALIZER.deserialize(in, available);
            }
            DAG dag = new DAG(oid, childCount, state, children, unpromotable, buckets);
            return dag;
        }

    }

    @Override
    public void save(TreeId bucketId, DAG bucketDAG) {
        trees.put(bucketId, bucketDAG);
    }

    @Override
    public TreeCache getTreeCache() {
        return treeCache;
    }

    @Override
    public long nodeCount() {
        // TODO Auto-generated method stub
        return 0;
    }

}
