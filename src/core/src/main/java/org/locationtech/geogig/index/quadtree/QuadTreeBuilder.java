package org.locationtech.geogig.index.quadtree;

import static com.google.common.collect.Lists.transform;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.eclipse.jdt.annotation.Nullable;
import org.geotools.data.DataUtilities;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.Decimator;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.locationtech.geogig.api.Bounded;
import org.locationtech.geogig.api.Bucket;
import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.api.RevTreeImpl;
import org.locationtech.geogig.storage.ObjectStore;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequenceFactory;
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier;

public class QuadTreeBuilder {

    static final int SIZE_SOFT_LIMIT = 64;

    private static final int DEFAULT_MAX_DEPTH = 12;

    private static final GeometryFactory GF = new GeometryFactory(
            PackedCoordinateSequenceFactory.FLOAT_FACTORY);

    private QuadTree root;

    public QuadTreeBuilder(Envelope maxBounds) {
        this(maxBounds, DEFAULT_MAX_DEPTH);
    }

    public QuadTreeBuilder(Envelope maxBounds, int maxDepth) {
        this.root = new QuadTree(maxBounds, maxDepth, SIZE_SOFT_LIMIT);
    }

    public int getDepth() {
        return root.getDepth();
    }

    public void put(Node featureNode) {
        Envelope env = new Envelope();
        featureNode.expand(env);
        if (env.isNull()) {
            return;
        }

        LeafNode node = new LeafNode(featureNode);
        root.put(node);
    }

    public void print(PrintStream out) {
        root.print(out);
    }

    Stopwatch unionTime = Stopwatch.createUnstarted();

    Stopwatch renderTime = Stopwatch.createUnstarted();

    public RevTree createRevTree(ObjectStore target) {

        pack();

        RevTree tree = createTree(root, target);

        System.err.printf("Simplification: %s, Union: %s (%,d geoms)\n", renderTime, unionTime,
                generalizationCount);
        return tree;

    }

    private static final Function<LeafNode, Node> NodeExtractor = new Function<LeafNode, Node>() {
        @Override
        public Node apply(LeafNode ln) {
            return ln.getNode();
        }
    };

    /**
     * Sets up the affine transform
     * <p/>
     * ((Taken from the old LiteRenderer))
     * 
     * @param mapExtent the map extent
     * @param paintArea the size of the rendering output area
     * @return a transform that maps from real world coordinates to the screen
     * 
     */
    public static AffineTransform worldToScreenTransform(Envelope mapExtent, Rectangle paintArea) {
        double scaleX = paintArea.getWidth() / mapExtent.getWidth();
        double scaleY = paintArea.getHeight() / mapExtent.getHeight();

        double tx = -mapExtent.getMinX() * scaleX;
        double ty = (mapExtent.getMinY() * scaleY) + paintArea.getHeight();

        AffineTransform at = new AffineTransform(scaleX, 0.0d, 0.0d, -scaleY, tx, ty);
        AffineTransform originTranslation = AffineTransform.getTranslateInstance(paintArea.x,
                paintArea.y);
        originTranslation.concatenate(at);

        return originTranslation != null ? originTranslation : at;
    }

    private RevTree createTree(QuadTree root, ObjectStore target) {

        if (root.isEmpty()) {
            return RevTree.EMPTY;
        }

        final RevTree tree;
        long totalSize = 0;

        // the higher level the tree, the bigger the depth
        // final int depth = root.getDepth();

        double generalizationDistance;
        double resolution;
        try {
            final Envelope maxBounds = root.quadrants.maxBounds;
            Rectangle paintArea = new Rectangle(256, 256);
            AffineTransform worldToScreenTransform = worldToScreenTransform(maxBounds, paintArea);
            MathTransform transform = ProjectiveTransform.create(worldToScreenTransform);

            // Maximum displacement for generalization during rendering
            generalizationDistance = 0.8;
            double[] spans = Decimator.computeGeneralizationDistances(transform.inverse(),
                    paintArea, generalizationDistance);
            generalizationDistance = spans[0] < spans[1] ? spans[0] : spans[1];
            resolution = Math.max(maxBounds.getWidth(), maxBounds.getHeight()) / 256d;
            // System.err.printf("Bounds: %s, distance: %f, res: %f\n", maxBounds,
            // generalizationDistance, resolution);
        } catch (TransformException e) {
            e.printStackTrace();
            throw Throwables.propagate(e);
        }

        final int nonPromotableLeafCount = root.nonPromotableLeaves.size();
        final int leafCount = root.leaves.size();

        // whish there was a Lists.concat method
        final List<Node> nodes = ImmutableList.copyOf(Iterables.concat(
                transform(root.nonPromotableLeaves, NodeExtractor),
                transform(root.leaves(), NodeExtractor)));

        TreeMap<Integer, Bucket> bucketTrees = new TreeMap<>();
        if (root.quadrants.maxDepth > 0) {
            for (Quadrant quadrant : Quadrant.values()) {
                Integer bucketIndex = Integer.valueOf(quadrant.ordinal());
                QuadTree qt = root.getQuadrant(quadrant);
                if (!qt.isEmpty()) {
                    totalSize += qt.size();
                    RevTree quadrantRevTree = createTree(qt, target);
                    Supplier<Map<String, Object>> extraData = createExtraData(quadrantRevTree,
                            generalizationDistance, resolution, root);

                    Bucket bucket;
                    // bucket = Bucket.create(quadrantRevTree.getId(), qt.getBounds(), extraData);
                    // bucketTrees.put(bucketIndex, bucket);
                    throw new UnsupportedOperationException();
                }
            }
        }

        totalSize += nonPromotableLeafCount + leafCount;
        // RevTree leafTree = RevTreeImpl
        // .createMixedTree(ObjectId.NULL, totalSize, nodes, bucketTrees);
        // tree = hashObject(leafTree);
        //
        // if (!nodes.isEmpty() && !bucketTrees.isEmpty()) {
        //
        // ImmutableList<Node> features = tree.features().get();
        // ImmutableSortedMap<Integer, Bucket> buckets = tree.buckets().get();
        //
        // Preconditions.checkState(nodes.size() == features.size(),
        // "expected %d features, got %d", nodes.size(), features.size());
        // Preconditions.checkState(bucketTrees.size() == buckets.size(),
        // "expected %d buckets, got %d", bucketTrees.size(), buckets.size());
        // }
        //
        // target.put(tree);
        // return tree;
        throw new UnsupportedOperationException();
    }

    int generalizationCount = 0;

    private Supplier<Map<String, Object>> createExtraData(RevTree quadrantRevTree,
            double simplificationDistance, double resolution, QuadTree root) {

        Iterator<Node> children = quadrantRevTree.children();
        ImmutableSortedMap<Integer, Bucket> buckets = quadrantRevTree.buckets().or(
                ImmutableSortedMap.<Integer, Bucket> of());

        List<Geometry> points = new ArrayList<Geometry>();
        List<Geometry> nonPoints = new ArrayList<Geometry>();
        while (children.hasNext()) {
            Geometry geom = extractGeometry(children.next());
            if (geom == null) {
                continue;
            }
            (geom.getDimension() == 0 ? points : nonPoints).add(geom);
        }
        for (Bucket b : buckets.values()) {
            Geometry geom = extractGeometry(b);
            if (geom == null) {
                continue;
            }
            (geom.getDimension() == 0 ? points : nonPoints).add(geom);
        }

        Supplier<Map<String, Object>> extraData = null;

        generalizationCount += points.size() + nonPoints.size();
        Geometry generalized = generalize(points, nonPoints, simplificationDistance);
        // Geometry generalized = generalizeWithRenrerer(points, nonPoints, simplificationDistance,
        // root.getBounds());
        if (generalized != null) {
            Map<String, Object> map = ImmutableMap.<String, Object> of("geometry", generalized,
                    "resolution", resolution);
            extraData = Suppliers.ofInstance(map);
        }
        return extraData;
    }

    private static CoordinateReferenceSystem DEFAULT_CRS;
    static {
        try {
            DEFAULT_CRS = CRS.decode("EPSG:4326", true);
        } catch (FactoryException e) {
            e.printStackTrace();
            throw Throwables.propagate(e);
        }
    }

    // private static final ExecutorService RENDER_EXECUTOR = Executors.newFixedThreadPool(4);
    //
    // private Geometry generalizeWithRenrerer(List<Geometry> points, List<Geometry> nonPoints,
    // double simplificationDistance, Envelope bounds) {
    //
    // StreamingRenderer renderer = new StreamingRenderer();
    // renderer.setThreadPool(RENDER_EXECUTOR);
    // MapContent mapContent = createMapContent(nonPoints);
    // renderer.setMapContent(mapContent);
    //
    // final List<Geometry> geometries = new ArrayList<>();
    // RenderListener listener = new RenderListener() {
    // @Override
    // public void featureRenderer(SimpleFeature feature) {
    // Geometry defaultGeometry = (Geometry) feature.getDefaultGeometry();
    // if (defaultGeometry != null) {
    // geometries.add(defaultGeometry);
    // }
    // }
    //
    // @Override
    // public void errorOccurred(Exception e) {
    // }
    // };
    // renderer.addRenderListener(listener);
    //
    // //renderer.setGeneralizationDistance(simplificationDistance);
    // Rectangle paintArea = new Rectangle(256, 256);
    // ReferencedEnvelope mapArea;
    // mapArea = new ReferencedEnvelope(DEFAULT_CRS);
    // mapArea.init(bounds);
    //
    // BufferedImage img = new BufferedImage(256, 256, BufferedImage.TYPE_BYTE_BINARY);
    // Graphics2D graphics = img.createGraphics();
    // try {
    // renderTime.start();
    // renderer.paint(graphics, paintArea, mapArea);
    // renderTime.stop();
    // } finally {
    // graphics.dispose();
    // mapContent.dispose();
    // }
    // Geometry geom = GF.buildGeometry(geometries);
    // unionTime.start();
    // //geom = geom.union();
    // unionTime.stop();
    // geom = GF.buildGeometry(collapse(geom));
    // return geom;
    // }
    //
    // private MapContent createMapContent(List<Geometry> nonPoints) {
    // MapContent map = new MapContent();
    // FeatureCollection collection = DataUtilities.collection(asFeatures(nonPoints));
    // Style style = createStyle();
    // Layer layer = new FeatureLayer(collection, style);
    // map.addLayer(layer);
    // return map;
    // }
    //
    // private Style createStyle() {
    // StyleBuilder b = new StyleBuilder();
    // return b.createStyle(b.createPolygonSymbolizer());
    // }

    private List<SimpleFeature> asFeatures(List<Geometry> geoms) {
        try {
            SimpleFeatureType type = DataUtilities.createType("temp", "geom:Geometry");
            final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(type);
            return ImmutableList.copyOf(Lists.transform(geoms,
                    new Function<Geometry, SimpleFeature>() {

                        int idc = 0;

                        @Override
                        public SimpleFeature apply(Geometry g) {
                            builder.reset();
                            builder.set(0, g);
                            return builder.buildFeature("fake" + idc++);
                        }
                    }));
        } catch (Exception e) {
            e.printStackTrace();
            throw Throwables.propagate(e);
        }
    }

    private Geometry generalize(List<Geometry> points, List<Geometry> nonPoints,
            double distanceTolerance) {

        // DouglasPeuckerSimplifier does not simplify point geometries
        Geometry geometry = reducePoints(points, distanceTolerance);

        if (!nonPoints.isEmpty()) {
            // simplifyTime.start();
            //
            // Geometry orig = nonPoints.get(0);
            // Geometry simpl = simplify(orig, distanceTolerance);
            // nonPoints.set(0, simpl);
            // if (!simpl.isEmpty()) {
            // for (int i = 1; i < nonPoints.size(); i++) {
            // Geometry g = simplify(nonPoints.get(i), distanceTolerance);
            // nonPoints.set(i, g);
            // }
            // }
            //
            // simplifyTime.stop();
            // geometry = GF.buildGeometry(nonPoints);
            // unionTime.start();
            // // unary union
            // geometry.union();
            // unionTime.stop();

            geometry = GF.buildGeometry(nonPoints);
            // unary union
            unionTime.start();
            // geometry.union();
            unionTime.stop();

            renderTime.start();
            geometry = simplify(geometry, distanceTolerance);
            renderTime.stop();

            geometry = GF.buildGeometry(collapse(geometry));
        }
        return geometry;
    }

    private List<Geometry> collapse(Geometry geometry) {

        List<Geometry> geomList = new ArrayList<>(geometry.getNumGeometries());
        for (int i = 0; i < geometry.getNumGeometries(); i++) {
            Geometry g = geometry.getGeometryN(i);
            if (g.getNumGeometries() > 1) {
                geomList.addAll(collapse(g));
            } else {
                geomList.add(g);
            }
        }
        return geomList;
    }

    private Geometry simplify(Geometry geometry, double distanceTolerance) {
        Geometry simplified = DouglasPeuckerSimplifier.simplify(geometry, distanceTolerance);
        if (simplified.isEmpty()) {
            CoordinateSequence coordinates = GF.getCoordinateSequenceFactory().create(5, 2);
            Envelope bounds = geometry.getEnvelopeInternal();
            coordinates.setOrdinate(0, 0, bounds.getMinX());
            coordinates.setOrdinate(0, 1, bounds.getMinY());
            coordinates.setOrdinate(1, 0, bounds.getMaxX());
            coordinates.setOrdinate(1, 1, bounds.getMinY());
            coordinates.setOrdinate(2, 0, bounds.getMaxX());
            coordinates.setOrdinate(2, 1, bounds.getMaxY());
            coordinates.setOrdinate(3, 0, bounds.getMinX());
            coordinates.setOrdinate(3, 1, bounds.getMaxY());
            coordinates.setOrdinate(4, 0, bounds.getMinX());
            coordinates.setOrdinate(4, 1, bounds.getMinY());
            simplified = GF.createPolygon(coordinates);
        }
        return simplified;
    }

    private Geometry reducePoints(List<Geometry> geoms, double distanceTolerance) {
        if (geoms.isEmpty()) {
            return GF.createPoint((CoordinateSequence) null);
        }
        if (geoms.size() == 1) {
            return geoms.get(0);
        }
        List<Point> points = new ArrayList<>(geoms.size());
        Geometry geom = geoms.get(0);
        {
            Coordinate[] coordinates = geom.getCoordinates();
            for (Coordinate c : coordinates) {
                points.add(GF.createPoint(c));
            }
        }
        for (int i = 1; i < geoms.size(); i++) {
            Geometry g = geoms.get(i);
            double distance = distance(g, points);
            if (distance > distanceTolerance) {
                Coordinate[] coordinates = g.getCoordinates();
                for (Coordinate c : coordinates) {
                    points.add(GF.createPoint(c));
                }
            } else {
                // System.err.printf("ignored %s\n", g);
            }
        }
        return GF.createMultiPoint(points.toArray(new Point[points.size()]));
    }

    private double distance(Geometry g, List<Point> points) {
        double distance = Double.MAX_VALUE;
        for (int i = 0; i < points.size(); i++) {
            Point p = points.get(i);
            distance = Math.min(distance, g.distance(p));
        }
        return distance;
    }

    @Nullable
    private Geometry extractGeometry(Bucket bucket) {
        // return extractGeometry(bucket.getExtraData(), bucket);
        throw new UnsupportedOperationException();
    }

    @Nullable
    private Geometry extractGeometry(Node node) {
        Geometry geometry = extractGeometry(node.getExtraData(), node);
        return geometry;
    }

    private Geometry extractGeometry(@Nullable Map<String, Object> extraData, Bounded b) {
        Geometry geom = null;
        if (extraData != null) {
            geom = (Geometry) extraData.get("geometry");
        }
        if (geom == null) {
            Optional<Envelope> bounds = b.bounds();
            if (bounds.isPresent()) {
                Envelope e = bounds.get();
                boolean isPoint = e.getWidth() == 0 && e.getHeight() == 0;
                if (isPoint) {
                    geom = GF.createPoint(new Coordinate(e.getMinX(), e.getMinY()));
                }
            }
        }

        return geom;
    }

    private RevTree hashObject(RevTree unnamedTree) {
        // ObjectId treeId = new HashObject().setObject(unnamedTree).call();

        ObjectId treeId = xorHash(unnamedTree);
        RevTreeImpl namedTree = RevTreeImpl.create(treeId, unnamedTree.size(), unnamedTree);
        return namedTree;
    }

    private ObjectId xorHash(RevTree tree) {
        final ObjectId initialId = RevTree.EMPTY_TREE_ID;

        byte[] result = initialId.getRawValue();

        Optional<ImmutableList<Node>> trees = tree.trees();
        Optional<ImmutableList<Node>> features = tree.features();
        Optional<ImmutableSortedMap<Integer, Bucket>> buckets = tree.buckets();
        if (trees.isPresent()) {
            for (Node n : trees.get()) {
                xor(n, result);
            }
        }
        if (features.isPresent()) {
            for (Node n : features.get()) {
                xor(n, result);
            }
        }

        if (buckets.isPresent()) {
            for (Bucket b : buckets.get().values()) {
                xor(b.getObjectId(), result);
            }
        }

        ObjectId id = ObjectId.createNoClone(result);
        return id;
    }

    private void xor(Node node, byte[] result) {
        xor(ObjectId.forString(node.getName()), result);
        xor(node.getObjectId(), result);
        if (node.getMetadataId().isPresent()) {
            xor(node.getMetadataId().get(), result);
        }
    }

    private void xor(ObjectId objectId, byte[] result) {
        for (int i = 0; i < ObjectId.NUM_BYTES; i++) {
            result[i] = (byte) ((int) result[i] ^ (int) objectId.byteN(i));
        }
    }

    public long size() {
        return root.size();
    }

    public void pack() {
        root.pack(true);
    }
}
