/*
 *  Copyright (c) 2017 Boundless and others.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Distribution License v1.0
 *  which accompanies this distribution, and is available at
 *  https://www.eclipse.org/org/documents/edl-v10.html
 *
 */

package org.locationtech.geogig.data;

import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.geogig.model.RevFeatureType;
import org.locationtech.geogig.plumbing.ResolveFeatureType;
import org.locationtech.geogig.porcelain.CommitOp;
import org.locationtech.geogig.repository.NodeRef;
import org.locationtech.geogig.test.integration.RepositoryTestCase;

import com.google.common.base.Optional;
import com.vividsolutions.jts.geom.Envelope;

public class EPSGBoundsYXTest extends RepositoryTestCase {

    @BeforeClass
    public static void setUp2() {
        System.setProperty("org.geotools.referencing.forceXY", "false");
    }
    @Override
    protected void setUpInternal() throws Exception {
        injector.configDatabase().put("user.name", "mthompson");
        injector.configDatabase().put("user.email", "mthompson@boundlessgeo.com");
    }

    @Test
    public void featureTypeTest() throws Exception {
        insertAndAdd(points1);
        geogig.command(CommitOp.class).setMessage("Commit1").call();

        Optional<RevFeatureType> featureType = geogig.command(ResolveFeatureType.class)
            .setRefSpec("WORK_HEAD:" + NodeRef.appendChild(pointsName, idP1)).call();

        Envelope bounds = new EPSGBoundsCalc().getCRSBounds(featureType);

        Envelope wgs84 = new Envelope(-90.0, 90.0, -180.0, 180.0);
        assertEquals(wgs84, bounds);
    }
}
