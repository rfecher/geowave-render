package org.vfny.geoserver.global;

import org.geoserver.feature.retype.RetypingFeatureSource;
import org.geoserver.feature.retype.RetypingFeatureSourceUtil;

public class GeoServerFeatureSourceUtil
{
	public static void unwrapRetypingFeatureSource(
			final GeoServerFeatureSource source ) {
		if (source.source instanceof RetypingFeatureSource) {
			source.source = RetypingFeatureSourceUtil.getWrappedFeatureSource((RetypingFeatureSource) source.source);
		}
	}
}
