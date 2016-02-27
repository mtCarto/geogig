package org.locationtech.geogig.index.quadtree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.eclipse.jdt.annotation.Nullable;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.operation.transform.IdentityTransform;
import org.geotools.renderer.ScreenMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.geogig.api.Bounded;
import org.locationtech.geogig.api.Bucket;
import org.locationtech.geogig.api.Context;
import org.locationtech.geogig.api.GlobalContextBuilder;
import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.api.RevObject.TYPE;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.api.RevTreeBuilder;
import org.locationtech.geogig.api.TestPlatform;
import org.locationtech.geogig.api.plumbing.DiffTree;
import org.locationtech.geogig.api.plumbing.diff.DiffEntry;
import org.locationtech.geogig.storage.NodeStorageOrder;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.storage.memory.HeapObjectDatabase;
import org.locationtech.geogig.test.integration.TestContextBuilder;
import org.opengis.referencing.operation.TransformException;

import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Range;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

@Ignore
public class QuadTreeBuilderTest {
    ObjectDatabase odb;

    @Before
    public void before() {
        odb = new HeapObjectDatabase();
        odb.open();
    }

    @After
    public void after() {
        odb.close();
    }

    @Test
    public void testSmallRects() throws IOException {
        Envelope maxBounds = new Envelope(-180, 180, -90, 90);
        int maxDepth = 8;
        final int ncount = 500_000;

        List<Node> nodes = createSmallRectNodes(nodeRange(ncount), maxBounds);

        QuadTreeBuilder sequentialTree = createQuadTree(maxBounds, maxDepth, nodes);
        assertTrue(String.format("expected size >= %,d, got %,d", ncount, sequentialTree.size()),
                sequentialTree.size() >= ncount);
        // sequentialTree.print(System.err);

        Collections.shuffle(nodes);

        // QuadTreeBuilder randomOrderTree = createQuadTree(maxBounds, maxDepth, nodes);
        // assertTrue(String.format("expected size >= %,d, got %,d", ncount,
        // randomOrderTree.size()),
        // randomOrderTree.size() >= ncount);
        // // randomOrderTree.print(System.out);
        //
        RevTree revTreeFromSequentialQuadTree = createRevTree(sequentialTree, odb);

        // for (Bucket b : revTreeFromSequentialQuadTree.buckets().get().values()) {
        // System.err.println(b.getExtraData().get("geometry"));
        // }

        // Geometry g = (Geometry)
        // revTreeFromSequentialQuadTree.buckets().get().get(0).getExtraData()
        // .get("geometry");
        // PrecisionModel pm = new PrecisionModel(1000);
        // GeometryFactory gf = new GeometryFactory(pm);
        // g = gf.createGeometry(g);
        // WKTWriter writer = new WKTWriter();
        // try (FileOutputStream out = new FileOutputStream("wkt.txt")) {
        // OutputStreamWriter w = new OutputStreamWriter(out);
        // writer.writeFormatted(g, w);
        // w.flush();
        // w.close();
        // }
        if (true)
            throw new UnsupportedOperationException();

        //
        // RevTree revTreeFromRandomQuadTree = createRevTree(randomOrderTree, odb);
        //
        // assertEquals(revTreeFromSequentialQuadTree, revTreeFromRandomQuadTree);
        // assertTrue(
        // String.format("expected size >= %,d, got %,d", ncount,
        // revTreeFromRandomQuadTree.size()),
        // revTreeFromRandomQuadTree.size() >= ncount);
    }

    @Test
    public void testTreeWithDifferentTopologyAndSameContentsHashTheSame() {
        final Envelope maxBounds = new Envelope(0, 100, 0, 100);
        final int ncount = 100_000;

        final List<Node> nodes = createSmallRectNodes(nodeRange(ncount), maxBounds);

        int maxDepth;
        maxDepth = 4;
        QuadTreeBuilder sequentialTree = createQuadTree(maxBounds, maxDepth, nodes);
        assertTrue(String.format("expected size >= %,d, got %,d", ncount, sequentialTree.size()),
                sequentialTree.size() >= ncount);
        assertEquals(maxDepth, sequentialTree.getDepth());
        // sequentialTree.print(System.err);

        Collections.shuffle(nodes);

        maxDepth = 3;
        QuadTreeBuilder randomOrderTree = createQuadTree(maxBounds, maxDepth, nodes);
        assertTrue(String.format("expected size >= %,d, got %,d", ncount, randomOrderTree.size()),
                randomOrderTree.size() >= ncount);
        assertEquals(maxDepth, randomOrderTree.getDepth());
        // randomOrderTree.print(System.out);

        RevTree revTreeFromSequentialQuadTree = createRevTree(sequentialTree, odb);
        RevTree revTreeFromRandomQuadTree = createRevTree(randomOrderTree, odb);

        assertEquals(revTreeFromSequentialQuadTree, revTreeFromRandomQuadTree);
        assertTrue(
                String.format("expected size >= %,d, got %,d", ncount,
                        revTreeFromRandomQuadTree.size()),
                revTreeFromRandomQuadTree.size() >= ncount);
    }

    @Test
    public void testRandomRects() {
        final Envelope maxBounds = new Envelope(-180, 180, -90, 90);
        final int maxDepth = 6;
        final int ncount = 100_000;

        List<Node> nodes = createRandomRectNodes(nodeRange(ncount), maxBounds);
        QuadTreeBuilder sequentialTree = createQuadTree(maxBounds, maxDepth, nodes);
        // sequentialTree.print(System.err);

        // Collections.shuffle(nodes);

        // QuadTreeBuilder randomOrderTree = createQuadTree(maxBounds, maxDepth, nodes);
        // randomOrderTree.print(System.out);

        ObjectDatabase odb = new HeapObjectDatabase();
        odb.open();

        RevTree revTreeFromSequentialQuadTree = createRevTree(sequentialTree, odb);
        for (Bucket b : revTreeFromSequentialQuadTree.buckets().get().values()) {
            // System.err.println(b.getExtraData());
        }
        //
        //
        // RevTree revTreeFromRandomQuadTree = createRevTree(randomOrderTree, odb);
        //
        // assertEquals(sequentialTree.size(), revTreeFromSequentialQuadTree.size());
        // assertEquals(randomOrderTree.size(), revTreeFromRandomQuadTree.size());
        //
        // assertEquals(revTreeFromSequentialQuadTree, revTreeFromRandomQuadTree);
        // assertTrue(
        // String.format("expected size >= %,d, got %,d", ncount,
        // revTreeFromRandomQuadTree.size()),
        // revTreeFromRandomQuadTree.size() >= ncount);
    }

    @Test
    public void testPoints() {
        final Envelope maxBounds = new Envelope(-180, 180, -90, 90);
        final int maxDepth = 3;
        final int ncount = 100_000;

        List<Node> nodes = createPointNodes(nodeRange(ncount), maxBounds);

        QuadTreeBuilder sequentialTree = createQuadTree(maxBounds, maxDepth, nodes);
        // sequentialTree.print(System.err);

        Collections.shuffle(nodes);

        QuadTreeBuilder randomOrderTree = createQuadTree(maxBounds, maxDepth, nodes);
        // randomOrderTree.print(System.out);

        ObjectDatabase odb = new HeapObjectDatabase();
        odb.open();

        RevTree revTreeFromSequentialQuadTree = createRevTree(sequentialTree, odb);
        RevTree revTreeFromRandomQuadTree = createRevTree(randomOrderTree, odb);

        assertEquals(sequentialTree.size(), revTreeFromSequentialQuadTree.size());
        assertEquals(randomOrderTree.size(), revTreeFromRandomQuadTree.size());

        assertEquals(revTreeFromSequentialQuadTree, revTreeFromRandomQuadTree);
        assertTrue(
                String.format("expected size >= %,d, got %,d", ncount,
                        revTreeFromRandomQuadTree.size()),
                revTreeFromRandomQuadTree.size() >= ncount);
    }

    @Test
    public void pointsPerformanceComparison() throws Exception {
        Envelope maxBounds = new Envelope(-180, 180, -90, 90);
        int maxDepth = 16;
        final int ncount = 20_000_000;

        Context context = createTestContext();
        ObjectDatabase odb = context.objectDatabase();
        odb.put(RevTree.EMPTY);

        RevTree quadTreeRevTree;
        RevTree regularRevTree;
        {
            List<Node> nodes;

            // nodes = createSmallRectNodes(nodeRange(ncount), maxBounds);
            nodes = createPointNodes(nodeRange(ncount), maxBounds);
            // nodes = createRandomRectNodes(nodeRange(ncount), maxBounds);

            QuadTreeBuilder quadTree = createQuadTree(maxBounds, maxDepth, nodes);
            // quadTree.print(System.out);
            System.err.println("Creating RevTree from QuadTree...");
            quadTreeRevTree = createRevTree(quadTree, odb);

            Collections.sort(nodes, new NodeStorageOrder());// make it easier for the tree
            // builder
            System.err.println("Creating regular tree...");
            regularRevTree = createRevTree(nodes, odb);
            // assertEquals(nodeIds.size(), regularRevTree.size());
            nodes.clear();
            nodes = null;
            quadTree = null;
            System.gc();
            Thread.sleep(1000);
        }

        Envelope queryEnvelope = new Envelope(30, 32, 30, 32);

        // warm up...
        System.err.println("Warming up...");
        search(regularRevTree, queryEnvelope, context);
        search(quadTreeRevTree, queryEnvelope, context);

        Stopwatch sw;

        System.err.println("Searching quad tree...");
        sw = Stopwatch.createStarted();
        long foundByQuadTree = search(quadTreeRevTree, queryEnvelope, context);
        System.err.printf("Search by %s took %s. Found %,d nodes.\n", queryEnvelope, sw.stop(),
                foundByQuadTree);

        System.err.println("Searching regular tree...");
        sw = Stopwatch.createStarted();
        long foundByRegularTree = search(regularRevTree, queryEnvelope, context);
        System.err.printf("Search by %s took %s. Found %,d nodes.\n", queryEnvelope, sw.stop(),
                foundByRegularTree);

        // System.err.println(foundByQuadTree);
        assertEquals(foundByRegularTree, foundByQuadTree);
    }

    @Test
    public void diffQuadTreeTest() throws Exception {
        Envelope maxBounds = new Envelope(-180, 180, -90, 90);
        int maxDepth = 16;
        final int ncount = 100_000;

        Context context = createTestContext();
        ObjectStore odb = context.objectDatabase();
        odb.put(RevTree.EMPTY);

        List<Node> nodes;
        // nodes = createSmallRectNodes(nodeIds, maxBounds);
        nodes = createPointNodes(nodeRange(ncount), maxBounds);
        // nodes = createRandomRectNodes(nodeIds, maxBounds);

        QuadTreeBuilder origQhadTree = createQuadTree(maxBounds, maxDepth, nodes);
        // quadTree.print(System.out);
        final RevTree revQTree1 = createRevTree(origQhadTree, odb);

        Node orig = nodes.get(1);
        Envelope bounds = new Envelope();
        orig.expand(bounds);
        bounds.expandBy(0.0001);
        Node change = Node.create(orig.getName(), ObjectId.forString("changes"), ObjectId.NULL,
                TYPE.FEATURE, bounds);
        nodes.set(1, change);
        // nodes.remove(2000);
        // nodes.remove(200);
        // nodes.remove(20);
        // nodes.remove(2);

        System.err.printf("orig: %s, new: %s\n", orig, change);

        QuadTreeBuilder changedQhadTree = createQuadTree(maxBounds, maxDepth, nodes);
        final RevTree revQTree2 = createRevTree(changedQhadTree, odb);

        Iterator<DiffEntry> diffs = context.command(DiffTree.class).setOldTree(revQTree1.getId())
                .setNewTree(revQTree2.getId()).call();
        while (diffs.hasNext()) {
            DiffEntry next = diffs.next();
            System.err.println(
                    next.changeType() + ": " + next.getOldObject() + " --- " + next.getNewObject());
        }
    }

    private List<Integer> nodeRange(final int ncount) {
        List<Integer> nodeIds = new ArrayList<>(ContiguousSet
                .create(Range.closedOpen(0, ncount), DiscreteDomain.integers()).asList());
        return nodeIds;
    }

    private static class ScreenMapFilter implements Predicate<Bounded> {

        static final class Stats {
            private long skippedTrees, skippedBuckets, skippedFeatures;

            private long acceptedTrees, acceptedBuckets, acceptedFeatures;

            void add(final Bounded b, final boolean skip) {
                Node n = b instanceof Node ? (Node) b : null;
                Bucket bucket = b instanceof Bucket ? (Bucket) b : null;
                if (skip) {
                    if (bucket == null) {
                        if (n.getType() == TYPE.FEATURE) {
                            skippedFeatures++;
                        } else {
                            skippedTrees++;
                        }
                    } else {
                        skippedBuckets++;
                    }
                } else {
                    if (bucket == null) {
                        if (n.getType() == TYPE.FEATURE) {
                            acceptedFeatures++;
                        } else {
                            acceptedTrees++;
                        }
                    } else {
                        acceptedBuckets++;
                    }
                }
            }

            @Override
            public String toString() {
                return String.format(
                        "skipped/accepted: Features(%,d/%,d) Buckets(%,d/%,d) Trees(%,d/%,d)",
                        skippedFeatures, acceptedFeatures, skippedBuckets, acceptedBuckets,
                        skippedTrees, acceptedTrees);
            }
        }

        private ScreenMap screenMap;

        private Envelope envelope = new Envelope();

        private Stats stats = new Stats();

        public ScreenMapFilter(ScreenMap screenMap) {
            this.screenMap = screenMap;
        }

        public Stats stats() {
            return stats;
        }

        @Override
        public boolean apply(@Nullable Bounded b) {
            if (b == null) {
                return false;
            }
            envelope.setToNull();
            b.expand(envelope);
            if (envelope.isNull()) {
                return true;
            }
            boolean skip;
            try {
                skip = screenMap.checkAndSet(envelope);
            } catch (TransformException e) {
                e.printStackTrace();
                return true;
            }
            stats.add(b, skip);
            return !skip;
        }

    }

    private long search(RevTree tree, Envelope queryEnvelope, Context context) {

        ScreenMap screenMap = new ScreenMap(-180, -90, 360, 180);
        screenMap.setTransform(IdentityTransform.create(2));
        screenMap.setSpans(2, 2);
        ScreenMapFilter screenmapFilter = new ScreenMapFilter(screenMap);

        DiffTree diff = new DiffTree();
        diff.setContext(context);
        diff.setOldTree(RevTree.EMPTY_TREE_ID);
        diff.setNewTree(tree.getId());

        // diff.setCustomFilter(screenmapFilter);

        diff.setBoundsFilter(new ReferencedEnvelope(queryEnvelope, DefaultGeographicCRS.WGS84));

        Iterator<DiffEntry> entries = diff.call();

        Stopwatch sw = Stopwatch.createStarted();
        long matchCount = Iterators.size(entries);
        sw.stop();
        // System.out.println(screenmapFilter.stats().toString());
        return matchCount;
    }

    protected Context createTestContext() {
        Platform testPlatform = new TestPlatform(new File("target"));
        GlobalContextBuilder.builder(new TestContextBuilder(testPlatform));
        Context context = GlobalContextBuilder.builder().build();
        context.objectDatabase().open();
        context.refDatabase().create();

        return context;
    }

    private List<Node> createPointNodes(List<Integer> nodeIds, Envelope maxBounds) {

        final double minX = maxBounds.getMinX();
        final double minY = maxBounds.getMinY();

        Stopwatch nodeTime = Stopwatch.createUnstarted();

        List<Node> nodes = new ArrayList<Node>(nodeIds.size());
        // List<Geometry> geoms = new ArrayList<Geometry>(nodeIds.size());
        GeometryFactory gf = new GeometryFactory();
        Random random = new Random();
        for (Integer intId : nodeIds) {
            nodeTime.start();
            String nodeName = String.valueOf(intId);
            String sid = Strings.padStart(nodeName, 40, '0');
            ObjectId oid = ObjectId.valueOf(sid);

            double x = minX + maxBounds.getWidth() * random.nextDouble();
            double y = minY + maxBounds.getHeight() * random.nextDouble();
            // geoms.add(gf.createPoint(new Coordinate(x, y)));
            Envelope bounds = new Envelope(x, x, y, y);

            Node node = Node.create(nodeName, oid, ObjectId.NULL, TYPE.FEATURE, bounds);
            nodeTime.stop();
            nodes.add(node);
        }
        // System.err.println(gf.buildGeometry(geoms));
        System.err.printf("%,d unique nodes created in %s.\n", nodeIds.size(), nodeTime);
        return nodes;
    }

    private List<Node> createSmallRectNodes(List<Integer> nodeIds, Envelope maxBounds) {

        final double minX = maxBounds.getMinX();
        final double minY = maxBounds.getMinY();
        final double stepx = maxBounds.getWidth() / nodeIds.size();
        final double stepy = maxBounds.getHeight() / nodeIds.size();

        Stopwatch nodeTime = Stopwatch.createUnstarted();

        List<Node> nodes = new ArrayList<Node>(nodeIds.size());

        Random random = new Random();

        GeometryFactory gf = new GeometryFactory();
        // List<Geometry> geoms = new ArrayList<Geometry>(nodeIds.size());
        for (Integer intId : nodeIds) {
            nodeTime.start();
            String nodeName = String.valueOf(intId);
            String sid = Strings.padStart(nodeName, 40, '0');
            ObjectId oid = ObjectId.forString(sid);

            double x1 = minX + (intId * stepx);
            double x2 = minX + (intId * stepx) + stepx;
            double y1 = minY + maxBounds.getHeight() * random.nextDouble();
            double y2 = y1 + stepy;
            Envelope bounds = new Envelope(x1, x2, y1, y2);

            Polygon geometry = JTS.toGeometry(bounds, gf);
            Map<String, Object> extraData = ImmutableMap.<String, Object> of("geometry", geometry);
            Node node = Node.create(nodeName, oid, ObjectId.NULL, TYPE.FEATURE, bounds, extraData);
            nodeTime.stop();
            nodes.add(node);
        }
        System.err.printf("%,d unique nodes created in %s.\n", nodeIds.size(), nodeTime);
        // System.err.println(gf.buildGeometry(geoms));
        return nodes;
    }

    private List<Node> createRandomRectNodes(List<Integer> nodeIds, Envelope maxBounds) {

        final double minX = maxBounds.getMinX();
        final double minY = maxBounds.getMinY();
        final double maxX = maxBounds.getMaxX();
        final double maxY = maxBounds.getMaxY();
        final double maxWidth = maxBounds.getWidth();
        final double maxHeight = maxBounds.getHeight();

        Random random = new Random();

        Stopwatch nodeTime = Stopwatch.createUnstarted();

        GeometryFactory gf = new GeometryFactory();

        List<Node> nodes = new ArrayList<Node>(nodeIds.size());

        for (Integer intId : nodeIds) {
            nodeTime.start();
            String sid = Strings.padStart(String.valueOf(intId), 40, '0');
            ObjectId oid = ObjectId.valueOf(sid);

            double x1 = minX + maxWidth * random.nextDouble();
            double y1 = minY + maxHeight * random.nextDouble();
            double x2 = Math.min(maxX, x1 + (maxWidth / 4) * random.nextDouble());
            double y2 = Math.min(maxY, y1 + (maxHeight / 4) * random.nextDouble());

            Envelope bounds = new Envelope(x1, x2, y1, y2);

            Polygon geometry = JTS.toGeometry(bounds, gf);
            Map<String, Object> extraData = ImmutableMap.<String, Object> of("geometry", geometry);

            Node node = Node.create(String.valueOf(intId), oid, ObjectId.NULL, TYPE.FEATURE, bounds,
                    extraData);
            nodes.add(node);
            nodeTime.stop();

            // geoms.add(JTS.toGeometry(bounds, gf));
        }

        // System.err.println(gf.buildGeometry(geoms));
        System.err.printf("%,d unique random rect nodes created in %s.\n", nodeIds.size(),
                nodeTime);
        return nodes;
    }

    private RevTree createRevTree(QuadTreeBuilder quadTree, ObjectStore odb) {
        Stopwatch treeTime = Stopwatch.createStarted();
        RevTree revTreeFromSequentialQuadTree = quadTree.createRevTree(odb);
        treeTime.stop();
        System.err.printf("RevTree created from QuadTree in %s: %s\n", treeTime,
                revTreeFromSequentialQuadTree);
        return revTreeFromSequentialQuadTree;
    }

    private RevTree createRevTree(List<Node> nodes, ObjectDatabase odb) {
        RevTreeBuilder builder = new RevTreeBuilder(odb);
        Stopwatch treeTime = Stopwatch.createStarted();
        for (Node n : nodes) {
            builder.put(n);
        }
        RevTree tree = builder.build();
        treeTime.stop();
        System.err.printf("Regular RevTree created in %s: %s\n", treeTime, tree);
        odb.put(tree);
        return tree;
    }

    private QuadTreeBuilder createQuadTree(Envelope maxBounds, int maxDepth,
            final List<Node> nodes) {
        System.err.printf("Creating QuadTree with %,d nodes...", nodes.size());

        QuadTreeBuilder qtree = new QuadTreeBuilder(maxBounds, maxDepth);

        Stopwatch sw = Stopwatch.createUnstarted();

        sw.start();
        for (Node node : nodes) {
            qtree.put(node);
        }

        qtree.pack();

        sw.stop();
        System.err.printf(" Created in %s. Depth %d\n", sw, qtree.getDepth());
        return qtree;
    }

}
