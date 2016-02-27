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

import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.api.RevTreeBuilder;
import org.locationtech.geogig.api.TreeBuilder;

import com.google.common.base.Stopwatch;

public class CanonicalTreeBuilderMapDBStorageTest extends CanonicalTreeBuilderTest {

    @Override
    protected TreeBuilder createBuiler() {
        return RevTreeBuilder.canonical(objectStore).withOnDiskStorage();
    }

    @Override
    protected TreeBuilder createBuiler(RevTree original) {
        return RevTreeBuilder.canonical(objectStore).original(original).withOnDiskStorage();
    }

    public static void main(String[] args) {

        CanonicalTreeBuilderMapDBStorageTest test = new CanonicalTreeBuilderMapDBStorageTest();
        try {
            test.setUp();
            Stopwatch sw = Stopwatch.createStarted();
            test.testBuildWithChildTreesSplittedRoot();
            sw.stop();
            System.err.printf("Run in %s\n", sw);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            test.after();
            System.exit(0);
        }
    }
}
