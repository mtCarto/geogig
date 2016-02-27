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

import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.api.RevTreeBuilder;
import org.locationtech.geogig.api.TreeBuilder;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CanonicalTreeBuilderPerformanceTestMapDB extends CanonicalTreeBuilderPerformanceTest {

    @Override
    protected TreeBuilder createBuiler(RevTree original) {
        return RevTreeBuilder.canonical(objectStore).original(original).withOnDiskStorage();
    }

}
