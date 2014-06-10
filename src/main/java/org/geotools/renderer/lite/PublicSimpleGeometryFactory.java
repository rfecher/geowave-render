package org.geotools.renderer.lite;

import com.vividsolutions.jts.geom.CoordinateSequenceFactory;

public class PublicSimpleGeometryFactory extends
		SimpleGeometryFactory
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public PublicSimpleGeometryFactory(
			final CoordinateSequenceFactory csFactory ) {
		super(
				csFactory);
	}
}
