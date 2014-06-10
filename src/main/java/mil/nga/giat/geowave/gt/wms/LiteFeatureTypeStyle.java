/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 * 
 *    (C) 2005-2008, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package mil.nga.giat.geowave.gt.wms;

import java.util.List;

import org.geotools.renderer.ScreenMap;
import org.geotools.styling.Rule;
import org.opengis.filter.expression.Expression;

public final class LiteFeatureTypeStyle
{
	public Rule[] ruleList;

	public Rule[] elseRules;

	public Expression transformation;

	/**
	 * The bit map used to decide whether to skip geometries that have been
	 * already drawn
	 */
	ScreenMap screenMap;

	public LiteFeatureTypeStyle(
			final List ruleList,
			final List elseRuleList,
			final Expression transformation ) {
		this.ruleList = (Rule[]) ruleList.toArray(new Rule[ruleList.size()]);
		elseRules = (Rule[]) elseRuleList.toArray(new Rule[elseRuleList.size()]);
		this.transformation = transformation;
	}

}
