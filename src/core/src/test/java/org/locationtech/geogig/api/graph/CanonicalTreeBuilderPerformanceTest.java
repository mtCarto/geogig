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

import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runners.MethodSorters;
import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.RevObject.TYPE;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.api.TreeBuilder;
import org.locationtech.geogig.api.plumbing.diff.RevObjectTestSupport;
import org.locationtech.geogig.storage.NodePathStorageOrder;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.storage.memory.HeapObjectStore;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.primitives.UnsignedLong;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class CanonicalTreeBuilderPerformanceTest {

    @Rule
    public TestName testName = new TestName();

    protected ObjectStore objectStore;

    protected TreeBuilder createBuiler() {
        return createBuiler(RevTree.EMPTY);
    }

    protected abstract TreeBuilder createBuiler(RevTree original);

    protected ObjectStore createObjectStore() {
        return new HeapObjectStore();
    }

    @Before
    public void setUp() throws Exception {
        objectStore = createObjectStore();
        objectStore.open();
    }

    @After
    public void after() {
        if (objectStore != null) {
            objectStore.close();
            objectStore = null;
        }
    }

    @Test
    public void test_01_Ramdom_100K() throws Exception {
        testRamdom(100_000);
    }

    @Test
    public void test_02_Sorted_100K() throws Exception {
        testSorted(100_000);
    }

    @Test
    public void test_03_Ramdom_1M() throws Exception {
        testRamdom(1100_000);
    }

    @Test
    public void test_04_Sorted_1M() throws Exception {
        testSorted(1100_000);
    }

    @Test
    @Ignore
    public void test_05_Ramdom_10M() throws Exception {
        testRamdom(10_100_000);
    }

    @Test
    @Ignore
    public void test_06_Ramdom_25M() throws Exception {
        testRamdom(25_000_000);
    }

    private RevTree testRamdom(final int featureCount) {
        Iterator<Integer> set = ContiguousSet.create(Range.closedOpen(0, featureCount),
                DiscreteDomain.integers()).iterator();

        Iterator<Node> nodes = Iterators.transform(set,
                (i) -> RevObjectTestSupport.featureNode("f", i));

        return testCreateTree(featureCount, nodes);
    }

    private RevTree testSorted(final int featureCount) {
        Iterable<Integer> set = ContiguousSet.create(Range.closedOpen(0, featureCount),
                DiscreteDomain.integers());

        Iterable<String> names = Iterables.transform(set, (i) -> i.toString());

        final SortedMap<UnsignedLong, String> sortedPrecomputedHashes = new TreeMap<>(
                Maps.uniqueIndex(names, (n) -> NodePathStorageOrder.INSTANCE.hashCodeLong(n)));

        Function<String, Node> f = (n) -> Node.create(n, ObjectId.forString(n), ObjectId.NULL,
                TYPE.FEATURE, null);

        Iterator<Node> nodes = Iterators.transform(sortedPrecomputedHashes.values().iterator(), f);

        return testCreateTree(featureCount, nodes);
    }

    private RevTree testCreateTree(final int featureCount, final Iterator<Node> nodes) {
        log("--- %s ---\n", testName.getMethodName());
        log("Creating tree with %,d nodes\n", featureCount);

        TreeBuilder builder = createBuiler();

        final Iterator<List<Node>> partitions = Iterators.partition(nodes, featureCount / 100);

        Stopwatch putTime = Stopwatch.createUnstarted();
        log("\tpopulating builder: ");
        int i = 0;
        while (partitions.hasNext()) {
            List<Node> partition = partitions.next();
            putTime.start();
            for (Node node : partition) {
                builder.put(node);
            }
            putTime.stop();
            i += partition.size();
            System.err.print(".");
        }
        log("\n\tpopulated in %s\n", putTime);

        Stopwatch buildTime = Stopwatch.createUnstarted();
        log("\tbuilding tree...");
        buildTime.start();
        RevTree tree = builder.build();
        buildTime.stop();
        log(" built in %s: %s\n", buildTime, tree);
        Assert.assertEquals(featureCount, tree.size());
        return tree;

    }

    private static void log(String fmt, Object... args) {
        System.err.printf(fmt, args);
    }
}
