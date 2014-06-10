package org.geotools.renderer.lite;

import org.opengis.filter.FilterFactory;
import org.opengis.filter.expression.PropertyName;

import com.vividsolutions.jts.geom.Envelope;

public class PublicFastBBOX extends
		FastBBOX
{

	public PublicFastBBOX(
			final PropertyName propertyName,
			final Envelope env,
			final FilterFactory factory ) {
		super(
				propertyName,
				env,
				factory);
	}

}
