package mil.nga.giat.geowave.gt.wms.accumulo;

import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.awt.geom.NoninvertibleTransformException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

import mil.nga.giat.geowave.gt.DistributableRenderer;
import mil.nga.giat.geowave.gt.datastore.GeoWaveGTDataStore;
import mil.nga.giat.geowave.gt.query.row.AbstractRowProvider;
import mil.nga.giat.geowave.gt.query.row.BasicRowIdStore;
import mil.nga.giat.geowave.gt.query.row.CountThresholdRowIdStore;
import mil.nga.giat.geowave.gt.query.row.DecimatedRenderingRowProvider;
import mil.nga.giat.geowave.gt.query.row.DecomposedQueryRangeRowProvider;
import mil.nga.giat.geowave.gt.query.row.ImageDecimator;
import mil.nga.giat.geowave.gt.wms.DelayedBackbufferGraphic;
import mil.nga.giat.geowave.gt.wms.RenderUtils;
import mil.nga.giat.geowave.index.NumericIndexStrategy;
import mil.nga.giat.geowave.index.PersistenceUtils;

import org.apache.log4j.Logger;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.Decimator;
import org.geotools.geometry.jts.GeometryClipper;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.LiteCoordinateSequence;
import org.geotools.geometry.jts.LiteCoordinateSequenceFactory;
import org.geotools.geometry.jts.LiteShape2;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.operation.transform.ConcatenatedTransform;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.renderer.ScreenMap;
import org.geotools.renderer.crs.ProjectionHandler;
import org.geotools.renderer.crs.ProjectionHandlerFinder;
import org.geotools.renderer.label.LabelCacheImpl;
import org.geotools.renderer.lite.PublicSymbolizerAssociation;
import org.geotools.renderer.lite.RendererUtilities;
import org.geotools.renderer.lite.StyledShapePainter;
import org.geotools.renderer.style.Style2D;
import org.geotools.styling.PointSymbolizer;
import org.geotools.styling.Rule;
import org.geotools.styling.StyleAttributeExtractor;
import org.geotools.styling.Symbolizer;
import org.geotools.styling.TextSymbolizer;
import org.geotools.util.NumberRange;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.PropertyName;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

public class ServerFeatureRenderer implements
		DistributableRenderer
{
	private final static Logger LOGGER = Logger.getLogger(ServerFeatureRenderer.class);
	LabelCacheImpl labelCache = new LabelCacheImpl();
	/** The painter class we use to depict shapes onto the screen */
	private final StyledShapePainter painter = new StyledShapePainter(
			labelCache);
	private final IdentityHashMap symbolizerAssociationHT = new IdentityHashMap();
	/**
	 * A decimator that will just transform coordinates
	 */
	private static final Decimator NULL_DECIMATOR = new Decimator(
			-1,
			-1);

	private final IdentityHashMap decimators = new IdentityHashMap();
	private final static FilterFactory2 filterFactory = CommonFactoryFinder.getFilterFactory2(null);
	private final static PropertyName defaultGeometryPropertyName = filterFactory.property("");
	/**
	 * Whether the renderer must perform generalization for the current set of
	 * features. For each layer we will set this flag depending on whether the
	 * datastore can do full generalization for us, or not
	 */
	private final boolean inMemoryGeneralization = true;

	/** Maximum displacement for generalization during rendering */
	private final double generalizationDistance = 0.8;

	private ServerPaintArea paintArea;
	private ServerMapArea mapArea;
	private List<ServerFeatureStyle> styles;
	private ServerRenderOptions renderOptions;
	private ServerDecimationOptions decimationOptions;

	private AffineTransform worldToScreenTransform = null;
	/**
	 * The handler that will be called to process the geometries to deal with
	 * projections singularities and dateline wrapping
	 */
	private ProjectionHandler projectionHandler;
	private NumberRange scaleRange;
	private final String layerId = "0";
	boolean postRenderComplete = false;

	protected ServerFeatureRenderer() {}

	public ServerFeatureRenderer(
			final ServerPaintArea paintArea,
			final ServerMapArea mapArea,
			final List<ServerFeatureStyle> styles,
			final ServerRenderOptions renderOptions,
			final ServerDecimationOptions decimationOptions ) {
		this.paintArea = paintArea;
		this.mapArea = mapArea;
		this.styles = styles;
		this.renderOptions = renderOptions;
		this.decimationOptions = decimationOptions;
	}

	public void init() {
		calculateWorldToScreen();
		scaleRange = NumberRange.create(
				renderOptions.scaleDenominator,
				renderOptions.scaleDenominator);
		renderOptions.init(paintArea);
		MathTransform transform = null;
		double[] spans = null;
		try {
			transform = ProjectiveTransform.create(worldToScreenTransform);
			spans = Decimator.computeGeneralizationDistances(
					transform.inverse(),
					paintArea.getArea(),
					generalizationDistance);
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Error computing the generalization hints",
					e);
		}
		for (final ServerFeatureStyle s : styles) {
			s.init(
					paintArea,
					renderOptions,
					transform,
					spans);
		}

		// enable advanced projection handling with the updated map extent
		if (renderOptions.advancedProjectionHandlingEnabled) {
			// get the projection handler and set a tentative envelope
			projectionHandler = ProjectionHandlerFinder.getHandler(
					mapArea.getBounds(),
					renderOptions.continuousMapWrapping);
		}
		labelCache.start();
		if (labelCache instanceof LabelCacheImpl) {
			labelCache.setLabelRenderingMode(renderOptions.labelRenderingMode);
		}

		labelCache.startLayer(layerId);
	}

	private synchronized void postRender() {
		if (!postRenderComplete) {
			labelCache.endLayer(
					layerId,
					renderOptions.labelGraphics,
					paintArea.getArea());
			if (!labelCache.getActiveLabels().isEmpty()) {
				renderOptions.labelGraphics.init();
				labelCache.end(
						renderOptions.labelGraphics,
						paintArea.getArea());
			}
			postRenderComplete = true;
		}
	}

	@Override
	public void render(
			final Object content )
			throws Exception {
		for (final ServerFeatureStyle style : styles) {
			process(
					style,
					content);
		}
	}

	private void calculateWorldToScreen() {
		final Rectangle paintRect = new Rectangle(
				0,
				0,
				paintArea.getWidth(),
				paintArea.getHeight());
		final ReferencedEnvelope dataArea = mapArea.getBounds();
		if (renderOptions.angle != 0.0) {
			worldToScreenTransform = new AffineTransform();
			worldToScreenTransform.translate(
					paintRect.width / 2,
					paintRect.height / 2);
			worldToScreenTransform.rotate(Math.toRadians(renderOptions.angle));
			worldToScreenTransform.translate(
					-paintRect.width / 2,
					-paintRect.height / 2);
			worldToScreenTransform.concatenate(RendererUtilities.worldToScreenTransform(
					dataArea,
					paintRect));
		}
		else {
			worldToScreenTransform = RendererUtilities.worldToScreenTransform(
					dataArea,
					paintRect);
		}
	}

	private void process(
			final ServerFeatureStyle style,
			final Object content )
			throws Exception {
		final List<Geometry> geometries = new ArrayList<Geometry>();
		final List<LiteShape2> shapes = new ArrayList<LiteShape2>();
		boolean doElse = true;
		final Rule[] elseRuleList = style.elseRules;
		final Rule[] ruleList = style.ruleList;
		Rule r;
		Filter filter;
		final Graphics2D graphics = style.graphics;
		final ScreenMap screenMap = style.getScreenMap();
		// applicable rules
		final int length = ruleList.length;
		for (int t = 0; t < length; t++) {
			r = ruleList[t];
			filter = r.getFilter();

			if ((filter == null) || filter.evaluate(content)) {
				doElse = false;
				processSymbolizers(
						graphics,
						screenMap,
						content,
						r.symbolizers(),
						scaleRange,
						geometries,
						shapes);
			}
		}

		if (doElse) {
			final int elseLength = elseRuleList.length;
			for (int tt = 0; tt < elseLength; tt++) {
				r = elseRuleList[tt];

				processSymbolizers(
						graphics,
						screenMap,
						content,
						r.symbolizers(),
						scaleRange,
						geometries,
						shapes);
			}
		}
	}

	private static com.vividsolutions.jts.geom.Geometry findGeometry(
			final Object drawMe,
			final Symbolizer s ) {
		final Expression geomExpr = s.getGeometry();

		// get the geometry
		Geometry geom;
		if (geomExpr == null) {
			if (drawMe instanceof SimpleFeature) {
				geom = (Geometry) ((SimpleFeature) drawMe).getDefaultGeometry();
			}
			else if (drawMe instanceof Feature) {
				geom = (Geometry) ((Feature) drawMe).getDefaultGeometryProperty().getValue();
			}
			else {
				geom = defaultGeometryPropertyName.evaluate(
						drawMe,
						Geometry.class);
			}
		}
		else {
			geom = geomExpr.evaluate(
					drawMe,
					Geometry.class);
		}

		return geom;
	}

	private LiteShape2 getShape(
			final Symbolizer symbolizer,
			final ScreenMap screenMap,
			final Object content,
			final List<Geometry> geometries,
			final List<LiteShape2> shapes )
			throws FactoryException {
		// this method is a snippet generally extracted from
		// org.geotools.renderer.lite.StreamingRenderer
		// lines 3229-3318
		Geometry g = findGeometry(
				content,
				symbolizer); // pulls the geometry

		if (g == null) {
			return null;
		}

		try {
			// process screenmap if necessary (only do it once, the geometry
			// will be transformed simplified in place and the screenmap really
			// needs to play against the original coordinates, plus, once we
			// start drawing a geometry we want to apply all symbolizers on it)
			if ((screenMap != null) && !(symbolizer instanceof PointSymbolizer) && !(g instanceof Point) && (getGeometryIndex(
					g,
					geometries) == -1)) {
				final Envelope env = g.getEnvelopeInternal();
				if (screenMap.canSimplify(env)) {
					if (screenMap.checkAndSet(env)) {
						return null;
					}
					else {
						g = screenMap.getSimplifiedShape(
								env.getMinX(),
								env.getMinY(),
								env.getMaxX(),
								env.getMaxY(),
								g.getFactory(),
								g.getClass());
					}
				}
			}

			PublicSymbolizerAssociation sa = (PublicSymbolizerAssociation) symbolizerAssociationHT.get(symbolizer);
			MathTransform crsTransform = null;
			MathTransform atTransform = null;
			MathTransform fullTransform = null;
			if (sa == null) {
				sa = new PublicSymbolizerAssociation();
				sa.crs = (findGeometryCS(
						content,
						symbolizer));
				if (sa.crs == null) {
					sa.crs = GeoWaveGTDataStore.DEFAULT_CRS;
				}
				try {
					crsTransform = RenderUtils.buildTransform(
							sa.crs,
							mapArea.getCRS());
					atTransform = ProjectiveTransform.create(worldToScreenTransform);
					fullTransform = RenderUtils.buildFullTransform(
							sa.crs,
							mapArea.getCRS(),
							worldToScreenTransform);
				}
				catch (final Exception e) {
					// fall through
					LOGGER.warn(
							e.getLocalizedMessage(),
							e);
				}
				sa.xform = fullTransform;
				sa.crsxform = crsTransform;
				sa.axform = atTransform;

				symbolizerAssociationHT.put(
						symbolizer,
						sa);
			}

			// some shapes may be too close to projection boundaries to
			// get transformed, try to be lenient
			if (symbolizer instanceof PointSymbolizer) {
				// if the coordinate transformation will occurr in place on the
				// coordinate sequence
				if (!renderOptions.clone && (g.getFactory().getCoordinateSequenceFactory() instanceof LiteCoordinateSequenceFactory)) {
					// if the symbolizer is a point symbolizer we first get the
					// transformed
					// geometry to make sure the coordinates have been modified
					// once, and then
					// compute the centroid in the screen space. This is a side
					// effect of the
					// fact we're modifing the geometry coordinates directly, if
					// we don't get
					// the reprojected and decimated geometry we risk of
					// transforming it twice
					// when computing the centroid
					final LiteShape2 first = getTransformedShape(
							g,
							sa,
							geometries,
							shapes);
					if (first != null) {
						if (projectionHandler != null) {
							// at the same time, we cannot keep the geometry in
							// screen space because
							// that would prevent the advanced projection
							// handling to do its work,
							// to replicate the geometries across the datelines,
							// so we transform
							// it back to the original
							final Geometry tx = JTS.transform(
									first.getGeometry(),
									sa.xform.inverse());
							return getTransformedShape(
									RendererUtilities.getCentroid(tx),
									sa,
									geometries,
									shapes);
						}
						else {
							return getTransformedShape(
									RendererUtilities.getCentroid(g),
									null,
									geometries,
									shapes);
						}
					}
					else {
						return null;
					}
				}
				else {
					return getTransformedShape(
							RendererUtilities.getCentroid(g),
							sa,
							geometries,
							shapes);
				}
			}
			else {
				return getTransformedShape(
						g,
						sa,
						geometries,
						shapes);
			}
		}
		catch (final TransformException te) {
			LOGGER.warn(
					te.getLocalizedMessage(),
					te);
			return null;
		}
		catch (final AssertionError ae) {
			LOGGER.warn(
					ae.getLocalizedMessage(),
					ae);
			return null;
		}
	}

	/**
	 * Finds the geometric attribute coordinate reference system.
	 * 
	 * @param drawMe2
	 * 
	 * @param f
	 *            The feature
	 * @param s
	 *            The symbolizer
	 * @return The geometry requested in the symbolizer, or the default geometry
	 *         if none is specified
	 */
	private org.opengis.referencing.crs.CoordinateReferenceSystem findGeometryCS(
			final Object drawMe,
			final Symbolizer s ) {

		if (drawMe instanceof Feature) {
			final Feature f = (Feature) drawMe;
			final FeatureType schema = f.getType();

			final Expression geometry = s.getGeometry();

			final String geomName = null;
			if (geometry instanceof PropertyName) {
				return getAttributeCRS(
						(PropertyName) geometry,
						schema);
			}
			else if (geometry == null) {
				return getAttributeCRS(
						null,
						schema);
			}
			else {
				final StyleAttributeExtractor attExtractor = new StyleAttributeExtractor();
				geometry.accept(
						attExtractor,
						null);
				for (final PropertyName name : attExtractor.getAttributes()) {
					if (name.evaluate(schema) instanceof GeometryDescriptor) {
						return getAttributeCRS(
								name,
								schema);
					}
				}
			}
		}

		return null;
	}

	/**
	 * Finds the CRS of the specified attribute (or uses the default geometry
	 * instead)
	 * 
	 * @param geomName
	 * @param schema
	 * @return
	 */
	org.opengis.referencing.crs.CoordinateReferenceSystem getAttributeCRS(
			final PropertyName geomName,
			final FeatureType schema ) {
		if ((geomName == null) || "".equals(geomName.getPropertyName())) {
			final GeometryDescriptor geom = schema.getGeometryDescriptor();
			return geom.getType().getCoordinateReferenceSystem();
		}
		else {
			final GeometryDescriptor geom = (GeometryDescriptor) geomName.evaluate(schema);
			return geom.getType().getCoordinateReferenceSystem();
		}
	}

	private int getGeometryIndex(
			final Geometry g,
			final List<Geometry> geometries ) {
		for (int i = 0; i < geometries.size(); i++) {
			if (geometries.get(i) == g) {
				return i;
			}
		}
		return -1;
	}

	private LiteShape2 getTransformedShape(
			final Geometry originalGeom,
			final PublicSymbolizerAssociation sa,
			final List<Geometry> geometries,
			final List<LiteShape2> shapes )
			throws TransformException,
			FactoryException {

		// this method is a snippet generally extracted from
		// org.geotools.renderer.lite.StreamingRenderer
		// lines 3329-3395
		final int idx = getGeometryIndex(
				originalGeom,
				geometries);
		if (idx != -1) {
			return shapes.get(idx);
		}

		// we need to clone if the clone flag is high or if the coordinate
		// sequence is not the one we asked for
		Geometry geom = originalGeom;
		if (renderOptions.clone || !(geom.getFactory().getCoordinateSequenceFactory() instanceof LiteCoordinateSequenceFactory)) {
			final int dim = sa.crs != null ? sa.crs.getCoordinateSystem().getDimension() : 2;
			geom = LiteCoordinateSequence.cloneGeometry(
					geom,
					dim);
		}

		LiteShape2 shape;
		if ((projectionHandler != null) && (sa != null)) {
			// first generalize and transform the geometry into the rendering
			// CRS
			geom = projectionHandler.preProcess(
					sa.crs,
					geom);
			if (geom == null) {
				shape = null;
			}
			else {
				// first generalize and transform the geometry into the
				// rendering CRS
				Decimator d = getDecimator(sa.xform);
				d.decimateTransformGeneralize(
						geom,
						sa.crsxform);
				geom.geometryChanged();
				// then post process it (provide reverse transform if available)
				MathTransform reverse = null;
				if (sa.crsxform != null) {
					if ((sa.crsxform instanceof ConcatenatedTransform) && (((ConcatenatedTransform) sa.crsxform).transform1.getTargetDimensions() >= 3) && (((ConcatenatedTransform) sa.crsxform).transform2.getTargetDimensions() == 2)) {
						reverse = null; // We are downcasting 3D data to 2D data
										// so no inverse is available
					}
					else {
						try {
							reverse = sa.crsxform.inverse();
						}
						catch (final Exception cannotReverse) {
							reverse = null; // reverse transform not available
						}
					}
				}
				geom = projectionHandler.postProcess(
						reverse,
						geom);
				if (geom == null) {
					shape = null;
				}
				else {
					// apply the affine transform turning the coordinates into
					// pixels
					d = new Decimator(
							-1,
							-1);
					d.decimateTransformGeneralize(
							geom,
							sa.axform);

					// wrap into a lite shape
					geom.geometryChanged();
					shape = new LiteShape2(
							geom,
							null,
							null,
							false,
							false);
				}
			}
		}
		else {
			MathTransform xform = null;
			if (sa != null) {
				xform = sa.xform;
			}
			shape = new LiteShape2(
					geom,
					xform,
					getDecimator(xform),
					false,
					false);
		}
		// cache the result
		geometries.add(originalGeom);
		shapes.add(shape);
		return shape;
	}

	/**
	 * @throws org.opengis.referencing.operation.NoninvertibleTransformException
	 */
	private Decimator getDecimator(
			final MathTransform mathTransform ) {
		// returns a decimator that does nothing if the currently set
		// generalization
		// distance is zero (no generalization desired) or if the datastore has
		// already done full generalization at the desired level
		if ((generalizationDistance == 0) || !inMemoryGeneralization) {
			return NULL_DECIMATOR;
		}

		Decimator decimator = (Decimator) decimators.get(mathTransform);
		if (decimator == null) {
			try {
				if ((mathTransform != null) && !mathTransform.isIdentity()) {
					decimator = new Decimator(
							mathTransform.inverse(),
							paintArea.getArea(),
							generalizationDistance);
				}
				else {
					decimator = new Decimator(
							null,
							paintArea.getArea(),
							generalizationDistance);
				}
			}
			catch (final org.opengis.referencing.operation.NoninvertibleTransformException e) {
				decimator = new Decimator(
						null,
						paintArea.getArea(),
						generalizationDistance);
			}

			decimators.put(
					mathTransform,
					decimator);
		}
		return decimator;
	}

	private void processSymbolizers(
			final Graphics2D graphics,
			final ScreenMap screenMap,
			final Object content,
			final List<Symbolizer> symbolizers,
			final NumberRange scaleRange,
			final List<Geometry> geometries,
			final List<LiteShape2> shapes )
			throws Exception {
		for (final Symbolizer symbolizer : symbolizers) {
			// this is a snippet generally extracted from
			// org.geotools.renderer.lite.StreamingRenderer
			// lines 2769-2811

			// /////////////////////////////////////////////////////////////////
			//
			// FEATURE
			//
			// /////////////////////////////////////////////////////////////////
			LiteShape2 shape = getShape(
					symbolizer,
					screenMap,
					content,
					geometries,
					shapes);
			if (shape == null) {
				continue;
			}

			if ((symbolizer instanceof TextSymbolizer) && (content instanceof Feature)) {
				labelCache.put(
						layerId,
						(TextSymbolizer) symbolizer,
						(Feature) content,
						shape,
						scaleRange);
			}
			else {
				final Style2D style = renderOptions.styleFactory.createStyle(
						content,
						symbolizer,
						scaleRange);

				// clip to the visible area + the size of the symbolizer
				// (with some extra
				// to make sure we get no artefacts from polygon new
				// borders)
				final double size = RendererUtilities.getStyle2DSize(style);
				// take into account the meta buffer to try and clip all
				// geometries by the same
				// amount
				final double clipBuffer = Math.max(
						size / 2,
						renderOptions.metaBuffer) + 10;
				final Envelope env = new Envelope(
						paintArea.getMinX(),
						paintArea.getMaxX(),
						paintArea.getMinY(),
						paintArea.getMaxY());
				env.expandBy(clipBuffer);
				final GeometryClipper clipper = new GeometryClipper(
						env);
				final Geometry g = clipper.clip(
						shape.getGeometry(),
						false);
				if (g == null) {
					continue;
				}
				if (g != shape.getGeometry()) {
					shape = new LiteShape2(
							g,
							null,
							null,
							false);
				}
				paint(
						graphics,
						shape,
						style,
						renderOptions.scaleDenominator,
						symbolizer.hasOption("labelObstacle"));
			}

		}
	}

	private void paint(
			final Graphics2D graphics,
			final LiteShape2 shape,
			final Style2D style,
			final double scaleDenominator,
			final boolean labelObstacle ) {
		if (graphics instanceof DelayedBackbufferGraphic) {
			((DelayedBackbufferGraphic) graphics).init();
		}

		try {
			painter.paint(
					graphics,
					shape,
					style,
					scaleDenominator,
					labelObstacle);
		}
		catch (final Throwable t) {
			// TODO do we want to return any render issues back to the client?
			LOGGER.warn(
					"Error rendering shape",
					t);
		}
	}

	@Override
	public byte[] toBinary() {
		final byte[] paintAreaBinary = PersistenceUtils.toBinary(paintArea);
		final byte[] mapAreaBinary = PersistenceUtils.toBinary(mapArea);

		final List<byte[]> styleBinaries = new ArrayList<byte[]>(
				styles.size());
		int styleBinaryLength = 0;
		for (final ServerFeatureStyle style : styles) {
			final byte[] binary = PersistenceUtils.toBinary(style);
			styleBinaries.add(binary);
			styleBinaryLength += (binary.length + 4);

		}
		final byte[] renderOptionsBinary = PersistenceUtils.toBinary(renderOptions);

		final byte[] decimationOptionsBinary;
		if (decimationOptions != null) {
			decimationOptionsBinary = PersistenceUtils.toBinary(decimationOptions);
		}
		else {
			decimationOptionsBinary = new byte[0];
		}
		final ByteBuffer buf = ByteBuffer.allocate(paintAreaBinary.length + mapAreaBinary.length + styleBinaryLength + renderOptionsBinary.length + decimationOptionsBinary.length + 20);
		buf.putInt(paintAreaBinary.length);
		buf.put(paintAreaBinary);
		buf.putInt(mapAreaBinary.length);
		buf.put(mapAreaBinary);
		buf.putInt(styleBinaries.size());
		for (final byte[] styleBinary : styleBinaries) {
			buf.putInt(styleBinary.length);
			buf.put(styleBinary);
		}
		buf.putInt(renderOptionsBinary.length);
		buf.put(renderOptionsBinary);
		buf.putInt(decimationOptionsBinary.length);
		buf.put(decimationOptionsBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int paintAreaBinaryLength = buf.getInt();
		final byte[] paintAreaBinary = new byte[paintAreaBinaryLength];
		buf.get(paintAreaBinary);
		paintArea = PersistenceUtils.fromBinary(
				paintAreaBinary,
				ServerPaintArea.class);
		final int mapAreaBinaryLength = buf.getInt();
		final byte[] mapAreaBinary = new byte[mapAreaBinaryLength];
		buf.get(mapAreaBinary);
		mapArea = PersistenceUtils.fromBinary(
				mapAreaBinary,
				ServerMapArea.class);
		final int styleSize = buf.getInt();
		styles = new ArrayList<ServerFeatureStyle>(
				styleSize);
		for (int i = 0; i < styleSize; i++) {
			final int stylebinaryLength = buf.getInt();
			final byte[] styleBinary = new byte[stylebinaryLength];
			buf.get(styleBinary);
			styles.add(PersistenceUtils.fromBinary(
					styleBinary,
					ServerFeatureStyle.class));

		}
		final int renderOptionsBinaryLength = buf.getInt();
		final byte[] renderOptionsBinary = new byte[renderOptionsBinaryLength];
		buf.get(renderOptionsBinary);
		renderOptions = PersistenceUtils.fromBinary(
				renderOptionsBinary,
				ServerRenderOptions.class);

		final int decimationOptionsBinaryLength = buf.getInt();
		if (decimationOptionsBinaryLength > 0) {
			final byte[] decimationOptionsBinary = new byte[decimationOptionsBinaryLength];
			buf.get(decimationOptionsBinary);
			decimationOptions = PersistenceUtils.fromBinary(
					decimationOptionsBinary,
					ServerDecimationOptions.class);
		}
		else {
			decimationOptions = null;
		}
		init();
	}

	@Override
	public RenderedMaster getResult() {
		postRender();
		return renderOptions.getRenderedMaster(styles);
	}

	@Override
	public AbstractRowProvider newRowProvider(
			final NumericIndexStrategy indexStrategy ) {
		ReferencedEnvelope epsg4326Envelope;
		try {
			epsg4326Envelope = mapArea.getBounds().transform(
					GeoWaveGTDataStore.DEFAULT_CRS,
					true);
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to transform bounding box to EPSG:4326",
					e);
			epsg4326Envelope = mapArea.getBounds();
		}
		if (decimationOptions != null) {
			BasicRowIdStore rowIdStore;
			try {
				if (decimationOptions.getMaxCount() != null) {
					rowIdStore = new CountThresholdRowIdStore(
							paintArea.getWidth(),
							paintArea.getHeight(),
							indexStrategy,
							mapArea.getBounds(),
							decimationOptions.getMaxCount(),
							decimationOptions.getPixelSize());
				}
				else {
					rowIdStore = new BasicRowIdStore(
							paintArea.getWidth(),
							paintArea.getHeight(),
							indexStrategy,
							mapArea.getBounds(),
							decimationOptions.getPixelSize());
				}
				renderOptions.labelGraphics.setCallback(new ImageDecimator(
						rowIdStore,
						decimationOptions.getMaxAlpha()));
				if (decimationOptions.isUseSecondaryStyles()) {
					for (final ServerFeatureStyle style : styles) {
						style.graphics.setCallback(new ImageDecimator(
								rowIdStore,
								decimationOptions.getMaxAlpha()));
					}
				}
				else if (!styles.isEmpty()) {
					styles.get(styles.size() - 1).graphics.setCallback(new ImageDecimator(
							rowIdStore,
							decimationOptions.getMaxAlpha()));
				}
				return new DecimatedRenderingRowProvider(
						rowIdStore,
						indexStrategy,
						epsg4326Envelope);
			}
			catch (final NoninvertibleTransformException e) {
				LOGGER.warn(
						"Unable to perform distributed decimation, fallback is without decimation",
						e);
			}
		}
		return new DecomposedQueryRangeRowProvider(
				indexStrategy,
				epsg4326Envelope);
	}

	@Override
	public boolean isDecimationEnabled() {
		return decimationOptions != null;
	}
}
