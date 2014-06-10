package org.geoserver.feature.retype;

import org.geotools.data.simple.SimpleFeatureSource;

public class RetypingFeatureSourceUtil
{
	public static SimpleFeatureSource getWrappedFeatureSource(
			final RetypingFeatureSource source ) {
		return source.wrapped;
	}
}
