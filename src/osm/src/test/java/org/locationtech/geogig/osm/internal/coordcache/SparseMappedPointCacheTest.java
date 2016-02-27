package org.locationtech.geogig.osm.internal.coordcache;

import org.locationtech.geogig.api.Platform;

public class SparseMappedPointCacheTest extends PointCacheTest {

    @Override
    protected PointCache createCache(Platform platform) {
        return new SparseMappedPointCache(platform);
    }

}
