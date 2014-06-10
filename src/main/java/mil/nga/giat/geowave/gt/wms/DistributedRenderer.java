package mil.nga.giat.geowave.gt.wms;

import java.awt.AlphaComposite;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.RenderingHints.Key;
import java.awt.geom.AffineTransform;
import java.awt.geom.NoninvertibleTransformException;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.media.jai.Interpolation;
import javax.media.jai.JAI;
import javax.media.jai.PlanarImage;

import mil.nga.giat.geowave.gt.datastore.GeoWaveFeatureCollection;
import mil.nga.giat.geowave.gt.wms.accumulo.RenderedMaster;
import mil.nga.giat.geowave.gt.wms.accumulo.ServerDecimationOptions;
import mil.nga.giat.geowave.gt.wms.accumulo.ServerFeatureRenderer;
import mil.nga.giat.geowave.gt.wms.accumulo.ServerFeatureStyle;
import mil.nga.giat.geowave.gt.wms.accumulo.ServerMapArea;
import mil.nga.giat.geowave.gt.wms.accumulo.ServerPaintArea;
import mil.nga.giat.geowave.gt.wms.accumulo.ServerRenderOptions;

import org.geoserver.wms.DefaultWebMapService;
import org.geoserver.wms.WMSMapContent;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.processing.CoverageProcessor;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureSource;
import org.geotools.data.Query;
import org.geotools.data.crs.ForceCoordinateSystemFeatureResults;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.Hints;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.FeatureTypes;
import org.geotools.feature.SchemaException;
import org.geotools.filter.IllegalFilterException;
import org.geotools.filter.function.GeometryTransformationVisitor;
import org.geotools.filter.function.RenderingTransformation;
import org.geotools.filter.spatial.DefaultCRSFilterVisitor;
import org.geotools.filter.spatial.ReprojectingFilterVisitor;
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor;
import org.geotools.filter.visitor.SimplifyingFilterVisitor;
import org.geotools.filter.visitor.SpatialFilterVisitor;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.geometry.jts.Decimator;
import org.geotools.geometry.jts.LiteCoordinateSequenceFactory;
import org.geotools.geometry.jts.LiteShape2;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.map.DirectLayer;
import org.geotools.map.Layer;
import org.geotools.map.MapContent;
import org.geotools.map.MapContext;
import org.geotools.map.MapLayer;
import org.geotools.parameter.Parameter;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.matrix.XAffineTransform;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.renderer.RenderListener;
import org.geotools.renderer.ScreenMap;
import org.geotools.renderer.crs.ProjectionHandler;
import org.geotools.renderer.crs.ProjectionHandlerFinder;
import org.geotools.renderer.label.LabelCacheImpl.LabelRenderingMode;
import org.geotools.renderer.lite.GraphicsAwareDpiRescaleStyleVisitor;
import org.geotools.renderer.lite.LabelCache;
import org.geotools.renderer.lite.MetaBufferEstimator;
import org.geotools.renderer.lite.OpacityFinder;
import org.geotools.renderer.lite.PublicFastBBOX;
import org.geotools.renderer.lite.PublicSimpleGeometryFactory;
import org.geotools.renderer.lite.RendererUtilities;
import org.geotools.renderer.lite.StreamingRenderer;
import org.geotools.renderer.lite.StyledShapePainter;
import org.geotools.renderer.lite.gridcoverage2d.GridCoverageRenderer;
import org.geotools.renderer.style.SLDStyleFactory;
import org.geotools.renderer.style.Style2D;
import org.geotools.resources.coverage.FeatureUtilities;
import org.geotools.resources.image.ImageUtilities;
import org.geotools.styling.FeatureTypeStyle;
import org.geotools.styling.PointSymbolizer;
import org.geotools.styling.RasterSymbolizer;
import org.geotools.styling.Rule;
import org.geotools.styling.RuleImpl;
import org.geotools.styling.Style;
import org.geotools.styling.StyleAttributeExtractor;
import org.geotools.styling.Symbolizer;
import org.geotools.styling.visitor.DpiRescaleStyleVisitor;
import org.geotools.styling.visitor.DuplicatingStyleVisitor;
import org.geotools.styling.visitor.UomRescaleStyleVisitor;
import org.geotools.util.NumberRange;
import org.opengis.coverage.processing.Operation;
import org.opengis.coverage.processing.OperationNotFoundException;
import org.opengis.feature.Feature;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.feature.type.PropertyDescriptor;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.PropertyName;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.crs.SingleCRS;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.MathTransform2D;
import org.opengis.referencing.operation.TransformException;
import org.opengis.style.LineSymbolizer;
import org.opengis.style.PolygonSymbolizer;
import org.vfny.geoserver.global.GeoServerFeatureSource;
import org.vfny.geoserver.global.GeoServerFeatureSourceUtil;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
/**
 * Some of this had to be copied from StreamingRenderer due to visibility constraints for code re-use.  This is not their direct product but the following authors contributed to org.geotools.renderer.lite.StreamingRender 
 * 
 * @author James Macgill
 * @author dblasby
 * @author jessie eichar
 * @author Simone Giannecchini
 * @author Andrea Aime
 * @author Alessio Fabiani
 */
public class DistributedRenderer extends
		StreamingRenderer
{
	private final static int defaultMaxFiltersToSendToDatastore = 5; // default
	private static final int REPROJECTION_RASTER_GUTTER = 10;
	/** The logger for the rendering module. */
	private static final Logger LOGGER = org.geotools.util.logging.Logging.getLogger("org.geotools.rendering");
	/** Tolerance used to compare doubles for equality */
	private static final double TOLERANCE = 1e-6;

	int error = 0;
	private static boolean VECTOR_RENDERING_ENABLED_DEFAULT = false;

	/** Filter factory for creating bounding box filters */
	private final static FilterFactory2 filterFactory = CommonFactoryFinder.getFilterFactory2(null);

	private final static PropertyName gridPropertyName = filterFactory.property("grid");

	private final static PropertyName paramsPropertyName = filterFactory.property("params");

	// support for crop
	private static final CoverageProcessor PROCESSOR = CoverageProcessor.getInstance();

	// the crop and scale operations
	private static final Operation CROP = PROCESSOR.getOperation("CoverageCrop");
	private static final Operation SCALE = PROCESSOR.getOperation("Scale");

	private CoordinateReferenceSystem destinationCrs;
	private Map rendererHints = null;

	private AffineTransform worldToScreenTransform = null;

	private final String textRenderingModeDEFAULT = TEXT_RENDERING_STRING;
	private boolean canTransform;

	/**
	 * The meta buffer for the current layer
	 */
	private int metaBuffer;
	/**
	 * The MapContent instance which contains the layers and the bounding box
	 * which needs to be rendered.
	 */
	private MapContent mapContent;

	/**
	 * Flag which controls behaviour for applying affine transformation to the
	 * graphics object. If true then the transform will be concatenated to the
	 * existing transform. If false it will be replaced.
	 */
	private boolean concatTransforms = false;

	/**
	 * Geographic map extent, eventually expanded to consider buffer area around
	 * the map
	 */
	private ReferencedEnvelope mapExtent;

	/** Geographic map extent, as provided by the caller */
	private ReferencedEnvelope originalMapExtent;

	/**
	 * The handler that will be called to process the geometries to deal with
	 * projections singularities and dateline wrapping
	 */
	private ProjectionHandler projectionHandler;

	/** The size of the output area in output units. */
	protected Rectangle screenSize;

	private BlockingQueue<RenderingRequest> requests;
	/**
	 * The thread pool used to submit the painter workers.
	 */
	private ExecutorService threadPool;

	private PainterThread painterThread;
	/**
	 * This flag is set to false when starting rendering, and will be checked
	 * during the rendering loop in order to make it stop forcefully
	 */
	private volatile boolean renderingStopRequested = false;

	/** Maximum displacement for generalization during rendering */
	private double generalizationDistance = 0.8;

	/** Factory that will resolve symbolizers into rendered styles */
	private final SLDStyleFactory styleFactory = new SLDStyleFactory();

	/** The painter class we use to depict shapes onto the screen */
	private final StyledShapePainter painter = new StyledShapePainter(
			labelCache);

	private final List<RenderListener> renderListeners = new CopyOnWriteArrayList<RenderListener>();

	private RenderingHints java2dHints;

	private final int renderingBufferDEFAULT = 0;

	private final String scaleComputationMethodDEFAULT = SCALE_OGC;

	/**
	 * Renders features based on the map layers and their styles as specified in
	 * the map context using <code>setContext</code>.
	 * <p/>
	 * This version of the method assumes that paint area, envelope and
	 * worldToScreen transform are already computed. Use this method to avoid
	 * recomputation. <b>Note however that no check is performed that they are
	 * really in sync!<b/>
	 * 
	 * @param graphics
	 *            The graphics object to draw to.
	 * @param paintArea
	 *            The size of the output area in output units (eg: pixels).
	 * @param mapArea
	 *            the map's visible area (viewport) in map coordinates. Its
	 *            associate CRS is ALWAYS 2D
	 * @param worldToScreen
	 *            A transform which converts World coordinates to Screen
	 *            coordinates.
	 */
	public void paint(
			final Graphics2D graphics,
			final Rectangle paintArea,
			final ReferencedEnvelope mapArea,
			AffineTransform worldToScreen,
			final boolean useAlpha,
			final Color bgColor ) {
		// ////////////////////////////////////////////////////////////////////
		//
		// Check for null arguments, recompute missing ones if possible
		//
		// ////////////////////////////////////////////////////////////////////
		if (graphics == null) {
			LOGGER.severe("renderer passed null graphics argument");
			throw new NullPointerException(
					"renderer requires graphics");
		}
		else if (paintArea == null) {
			LOGGER.severe("renderer passed null paintArea argument");
			throw new NullPointerException(
					"renderer requires paintArea");
		}
		else if (mapArea == null) {
			LOGGER.severe("renderer passed null mapArea argument");
			throw new NullPointerException(
					"renderer requires mapArea");
		}
		else if (worldToScreen == null) {
			worldToScreen = RendererUtilities.worldToScreenTransform(
					mapArea,
					paintArea);
			if (worldToScreen == null) {
				return;
			}
		}

		// ////////////////////////////////////////////////////////////////////
		//
		// Setting base information
		//
		// TODO the way this thing is built is a mess if you try to use it in a
		// multithreaded environment. I will fix this at the end.
		//
		// ////////////////////////////////////////////////////////////////////
		destinationCrs = mapArea.getCoordinateReferenceSystem();
		mapExtent = new ReferencedEnvelope(
				mapArea);
		screenSize = paintArea;
		worldToScreenTransform = worldToScreen;
		error = 0;
		if (java2dHints != null) {
			graphics.setRenderingHints(java2dHints);
		}
		// add the anchor for graphic fills
		final Point2D textureAnchor = new Point2D.Double(
				worldToScreenTransform.getTranslateX(),
				worldToScreenTransform.getTranslateY());
		graphics.setRenderingHint(
				StyledShapePainter.TEXTURE_ANCHOR_HINT_KEY,
				textureAnchor);
		// reset the abort flag
		renderingStopRequested = false;

		// setup the graphic clip
		graphics.setClip(paintArea);

		// ////////////////////////////////////////////////////////////////////
		//
		// Managing transformations , CRSs and scales
		//
		// If we are rendering to a component which has already set up some form
		// of transformation then we can concatenate our transformation to it.
		// An example of this is the ZoomPane component of the swinggui module.
		// ////////////////////////////////////////////////////////////////////
		if (concatTransforms) {
			final AffineTransform atg = graphics.getTransform();
			atg.concatenate(worldToScreenTransform);
			worldToScreenTransform = atg;
			graphics.setTransform(worldToScreenTransform);
		}

		// compute scale according to the user specified method
		scaleDenominator = computeScale(
				mapArea,
				paintArea,
				worldToScreenTransform,
				rendererHints);
		if (LOGGER.isLoggable(Level.FINE)) {
			LOGGER.fine("Computed scale denominator: " + scaleDenominator);
		}
		// ////////////////////////////////////////////////////////////////////
		//
		// Consider expanding the map extent so that a few more geometries
		// will be considered, in order to catch those outside of the rendering
		// bounds whose stroke is so thick that it countributes rendered area
		//
		// ////////////////////////////////////////////////////////////////////
		final int buffer = getRenderingBuffer();
		originalMapExtent = mapExtent;
		if (buffer > 0) {
			mapExtent = new ReferencedEnvelope(
					expandEnvelope(
							mapExtent,
							worldToScreen,
							buffer),
					mapExtent.getCoordinateReferenceSystem());
		}

		// enable advanced projection handling with the updated map extent
		if (isAdvancedProjectionHandlingEnabled()) {
			// get the projection handler and set a tentative envelope
			projectionHandler = ProjectionHandlerFinder.getHandler(
					mapExtent,
					isMapWrappingEnabled());
		}

		// Setup the secondary painting thread
		requests = getRequestsQueue2();
		painterThread = new PainterThread(
				requests);
		ExecutorService localThreadPool = threadPool;
		boolean localPool = false;
		if (localThreadPool == null) {
			localThreadPool = Executors.newSingleThreadExecutor();
			localPool = true;
		}
		final Future painterFuture = localThreadPool.submit(painterThread);
		try {
			if (mapContent == null) {
				throw new IllegalStateException(
						"Cannot call paint, you did not set a MapContent in this renderer");
			}

			final int layersNumber = mapContent.layers().size();
			for (int i = 0; i < layersNumber; i++) // DJB: for each layer (ie.
													// one
			{
				final Layer layer = mapContent.layers().get(
						i);

				if (!layer.isVisible()) {
					// Only render layer when layer is visible
					continue;
				}

				if (renderingStopRequested) {
					return;
				}

				if (layer instanceof DirectLayer) {
					final RenderingRequest request = new RenderDirectLayerRequest(
							graphics,
							(DirectLayer) layer);
					try {
						requests.put(request);
					}
					catch (final InterruptedException e) {
						fireErrorEvent(e);
					}

				}
				else {
					final MapLayer currLayer = new MapLayer(
							layer);
					try {
						double angle = 0;
						if (mapContent instanceof WMSMapContent) {
							angle = ((WMSMapContent) mapContent).getAngle();
						}

						// extract the feature type stylers from the style
						// object and process them
						processStylers(
								graphics,
								currLayer,
								worldToScreenTransform,
								destinationCrs,
								mapExtent,
								screenSize,
								angle,
								bgColor,
								useAlpha,
								i + "");
					}
					catch (final Throwable t) {
						fireErrorEvent(t);
					}
				}
			}
		}
		finally {
			try {
				if (!renderingStopRequested) {
					requests.put(new EndRequest());
					painterFuture.get();
				}
			}
			catch (final Exception e) {
				painterFuture.cancel(true);
				fireErrorEvent(e);
			}
			finally {
				if (localPool) {
					localThreadPool.shutdown();
				}
			}
		}

		if (LOGGER.isLoggable(Level.FINE)) {
			LOGGER.fine(new StringBuffer(
					"Style cache hit ratio: ").append(
					styleFactory.getHitRatio()).append(
					" , hits ").append(
					styleFactory.getHits()).append(
					", requests ").append(
					styleFactory.getRequests()).toString());
		}
		if (error > 0) {
			LOGGER.warning(new StringBuffer(
					"Number of Errors during paint(Graphics2D, AffineTransform) = ").append(
					error).toString());
		}

	}

	/**
	 * Sets a thread pool to be used in parallel rendering
	 * 
	 * @param threadPool
	 */
	@Override
	public void setThreadPool(
			final ExecutorService threadPool ) {
		this.threadPool = threadPool;
	}

	/**
	 * Sets the flag which controls behaviour for applying affine transformation
	 * to the graphics object.
	 * 
	 * @param flag
	 *            If true then the transform will be concatenated to the
	 *            existing transform. If false it will be replaced.
	 */
	@Override
	public void setConcatTransforms(
			final boolean flag ) {
		concatTransforms = flag;
	}

	/**
	 * Flag which controls behaviour for applying affine transformation to the
	 * graphics object.
	 * 
	 * @return a boolean flag. If true then the transform will be concatenated
	 *         to the existing transform. If false it will be replaced.
	 */
	@Override
	public boolean getConcatTransforms() {
		return concatTransforms;
	}

	/**
	 * adds a listener that responds to error events of feature rendered events.
	 * 
	 * @see RenderListener
	 * 
	 * @param listener
	 *            the listener to add.
	 */
	@Override
	public void addRenderListener(
			final RenderListener listener ) {
		renderListeners.add(listener);
	}

	/**
	 * Removes a render listener.
	 * 
	 * @see RenderListener
	 * 
	 * @param listener
	 *            the listener to remove.
	 */
	@Override
	public void removeRenderListener(
			final RenderListener listener ) {
		renderListeners.remove(listener);
	}

	/**
	 * Builds the blocking queue used to bridge between the data loading thread
	 * and the painting one
	 * 
	 * @return
	 */
	protected BlockingQueue<RenderingRequest> getRequestsQueue2() {
		return new RenderingBlockingQueue(
				10000);
	}

	/**
	 * Applies each feature type styler in turn to all of the features. This
	 * perhaps needs some explanation to make it absolutely clear.
	 * featureStylers[0] is applied to all features before featureStylers[1] is
	 * applied. This can have important consequences as regards the painting
	 * order.
	 * <p>
	 * In most cases, this is the desired effect. For example, all line features
	 * may be rendered with a fat line and then a thin line. This produces a
	 * 'cased' effect without any strange overlaps.
	 * </p>
	 * <p>
	 * This method is internal and should only be called by render.
	 * </p>
	 * <p>
	 * </p>
	 * 
	 * @param graphics
	 *            DOCUMENT ME!
	 * @param features
	 *            An array of features to be rendered
	 * @param featureStylers
	 *            An array of feature stylers to be applied
	 * @param at
	 *            DOCUMENT ME!
	 * @param destinationCrs
	 *            - The destination CRS, or null if no reprojection is required
	 * @param screenSize
	 * @param layerId
	 * @throws IOException
	 * @throws IllegalFilterException
	 */
	private void processStylers(
			final Graphics2D graphics,
			final MapLayer currLayer,
			final AffineTransform at,
			final CoordinateReferenceSystem destinationCrs,
			final ReferencedEnvelope mapArea,
			final Rectangle screenSize,
			final double angle,
			final Color bgColor,
			final boolean useAlpha,
			final String layerId )
			throws Exception {
		final Object distributedRenderingEnabledObj = currLayer.toLayer().getUserData().get(
				DistributedRenderMapOutputFormat.DISTRIBUTED_RENDERING_KEY);
		final Object distributedDecimatedRenderingEnabledObj = currLayer.toLayer().getUserData().get(
				DistributedRenderMapOutputFormat.DISTRIBUTED_DECIMATED_RENDERING_KEY);
		ServerDecimationOptions decimationOptions = null;
		if ((distributedDecimatedRenderingEnabledObj != null) && (distributedDecimatedRenderingEnabledObj instanceof ServerDecimationOptions)) {
			decimationOptions = (ServerDecimationOptions) distributedDecimatedRenderingEnabledObj;
		}
		final boolean distributedRenderingEnabled = (decimationOptions != null) || ((distributedRenderingEnabledObj != null) && (distributedRenderingEnabledObj instanceof Boolean) && (Boolean) distributedRenderingEnabledObj);

		/*
		 * DJB: changed this a wee bit so that it now does the layer query AFTER
		 * it has evaluated the rules for scale inclusion. This makes it so that
		 * geometry columns (and other columns) will not be queried unless they
		 * are actually going to be required. see geos-469
		 */
		// /////////////////////////////////////////////////////////////////////
		//
		// Preparing feature information and styles
		//
		// /////////////////////////////////////////////////////////////////////
		final Style style = currLayer.getStyle();
		final FeatureSource featureSource = currLayer.getFeatureSource();

		final CoordinateReferenceSystem sourceCrs;
		final NumberRange scaleRange = NumberRange.create(
				scaleDenominator,
				scaleDenominator);
		final ArrayList<LiteFeatureTypeStyle> lfts;

		if (featureSource != null) {
			FeatureCollection features = null;
			final FeatureType schema = featureSource.getSchema();

			final GeometryDescriptor geometryAttribute = schema.getGeometryDescriptor();
			if ((geometryAttribute != null) && (geometryAttribute.getType() != null)) {
				sourceCrs = geometryAttribute.getType().getCoordinateReferenceSystem();
			}
			else {
				sourceCrs = null;
			}
			if (LOGGER.isLoggable(Level.FINE)) {
				LOGGER.fine("Processing " + style.featureTypeStyles().size() + " stylers for " + featureSource.getSchema().getName());
			}

			lfts = createLiteFeatureTypeStyles(
					style.featureTypeStyles(),
					schema,
					graphics);
			if (lfts.isEmpty()) {
				return;
			}

			// make sure all spatial filters in the feature source native SRS
			reprojectSpatialFilters(
					lfts,
					featureSource);

			// apply the uom and dpi rescale
			applyUnitRescale(lfts);

			// classify by transformation
			final List<List<LiteFeatureTypeStyle>> txClassified = classifyByTransformation(lfts);

			// render groups by uniform transformation
			for (final List<LiteFeatureTypeStyle> uniform : txClassified) {
				final Expression transform = uniform.get(0).transformation;

				final boolean hasTransformation = transform != null;
				final Query styleQuery = getStyleQuery(
						featureSource,
						schema,
						uniform,
						mapArea,
						destinationCrs,
						sourceCrs,
						screenSize,
						geometryAttribute,
						at,
						hasTransformation);

				// metabuffer is calculated in getStyleQuery(), so this is the
				// earliest point to instantiate the renderer
				final ServerFeatureRenderer renderer = getServerFeatureRenderer(
						lfts,
						screenSize,
						mapArea,
						angle,
						currLayer,
						graphics.getRenderingHints(),
						bgColor,
						metaBuffer,
						scaleDenominator,
						useAlpha,
						LabelRenderingMode.valueOf(getTextRenderingMethod()),
						decimationOptions);

				final Query definitionQuery = getDefinitionQuery(
						currLayer,
						featureSource,
						sourceCrs);

				if (distributedRenderingEnabled) {
					styleQuery.getHints().add(
							new RenderingHints(
									GeoWaveFeatureCollection.SERVER_FEATURE_RENDERER,
									renderer));
					if (featureSource instanceof GeoServerFeatureSource) {

						// sadly, when the layer name ("alias") doesn't match
						// the
						// native feature type name, geoserver will wrap the
						// feature
						// source as a RetypingFeatureSource...wrapped in a
						// GeoServerFeatureSource

						// if this is the case, ideally we would set
						// featureSource =
						// ((RetypingFeatureSource)featureSource).wrappedSource
						// to undo the wrapping but this is package private so
						// there is no visiblity to perform this

						// therefore we have utilities methods in the
						// appropriate packages to perform this unwrapping
						GeoServerFeatureSourceUtil.unwrapRetypingFeatureSource((GeoServerFeatureSource) featureSource);
					}

				}
				if (hasTransformation) {
					// prepare the stage for the raster transformations
					final GridGeometry2D gridGeometry = getRasterGridGeometry(
							destinationCrs,
							sourceCrs);
					// vector transformation wise, we have to account for two
					// separate queries,
					// the one attached to the layer and then one coming from
					// SLD.
					// The first source attributes, the latter talks tx output
					// attributes
					// so they have to be applied before and after the
					// transformation respectively

					features = applyRenderingTransformation(
							transform,
							featureSource,
							definitionQuery,
							styleQuery,
							gridGeometry,
							sourceCrs);
					if (features == null) {
						return;
					}
				}
				else {
					final Query mixed = DataUtilities.mixQueries(
							definitionQuery,
							styleQuery,
							null);
					checkAttributeExistence(
							featureSource.getSchema(),
							mixed);
					features = featureSource.getFeatures(mixed);
					features = prepFeatureCollection(
							features,
							sourceCrs);
				}

				// HACK HACK HACK
				// For complex features, we need the targetCrs and version in
				// scenario where we have
				// a top level feature that does not contain a
				// geometry(therefore no crs) and has a
				// nested feature that contains geometry as its
				// property.Furthermore it is possible
				// for each nested feature to have different crs hence we need
				// to reproject on each
				// feature accordingly.
				// This is a Hack, this information should not be passed through
				// feature type
				// appschema will need to remove this information from the
				// feature type again
				if (!(features instanceof SimpleFeatureCollection)) {
					features.getSchema().getUserData().put(
							"targetCrs",
							destinationCrs);
					features.getSchema().getUserData().put(
							"targetVersion",
							"wms:getmap");
				}
				// finally, perform rendering
				final FeatureIterator<Feature> featureIterator = features.features();
				final DistributedRenderResultStore resultStore = new DistributedRenderResultStore();
				if (distributedRenderingEnabled) {

					// the rendering has been distributed using a query hint,
					// expect the feature iterator to contain a fully rendered
					// result set
					while (featureIterator.hasNext()) {
						final Feature f = featureIterator.next();
						final Collection<Property> properties = f.getProperties();
						if ((properties != null) && !properties.isEmpty()) {
							final Property property = properties.iterator().next();
							// the renderedmaster property should be the only
							// one in the feature, but for safeties sake, check
							// the type
							if ((property != null) && (property.getValue() instanceof RenderedMaster)) {
								final RenderedMaster result = (RenderedMaster) property.getValue();
								resultStore.addResult(
										f.getIdentifier().getID(),
										result);
								continue;
							}
						}
					}
				}
				else {
					renderer.init();
					// we shouldn't distribute the rendering, just render all of
					// the features to a single server renderer and add the
					// result to the result store
					while (featureIterator.hasNext()) {
						renderer.render(featureIterator.next());
					}
					resultStore.addResult(
							"master",
							renderer.getResult());
				}
				featureIterator.close();
				final String[] styleIdDrawOrder = new String[lfts.size()];
				for (int i = 0; i < styleIdDrawOrder.length; i++) {
					styleIdDrawOrder[i] = Integer.toString(i);
				}
				requests.put(new MergeLayersRequest(
						graphics,
						resultStore.getDrawOrderResults(styleIdDrawOrder)));
			}
		}
	}

	private void fireFeatureRenderedEvent(
			final Object feature ) {
		if (!(feature instanceof SimpleFeature)) {
			if (feature instanceof Feature) {
				LOGGER.log(
						Level.FINE,
						"Skipping non simple feature rendering notification");
			}
			return;
		}
		if (renderListeners.size() > 0) {
			RenderListener listener;
			for (int i = 0; i < renderListeners.size(); i++) {
				listener = renderListeners.get(i);
				listener.featureRenderer((SimpleFeature) feature);
			}
		}
	}

	private void fireErrorEvent(
			final Throwable t ) {
		LOGGER.log(
				Level.SEVERE,
				t.getLocalizedMessage(),
				t);
		if (renderListeners.size() > 0) {
			Exception e;
			if (t instanceof Exception) {
				e = (Exception) t;
			}
			else {
				e = new Exception(
						t);
			}
			RenderListener listener;
			for (int i = 0; i < renderListeners.size(); i++) {
				listener = renderListeners.get(i);
				listener.errorOccurred(e);
			}
		}
	}

	private double computeScale(
			final ReferencedEnvelope envelope,
			final Rectangle paintArea,
			final AffineTransform worldToScreen,
			final Map hints ) {
		if (getScaleComputationMethod().equals(
				SCALE_ACCURATE)) {
			try {
				return RendererUtilities.calculateScale(
						envelope,
						paintArea.width,
						paintArea.height,
						hints);
			}
			catch (final Exception e) // probably either (1) no CRS (2) error
										// xforming
			{
				LOGGER.log(
						Level.WARNING,
						e.getLocalizedMessage(),
						e);
			}
		}
		if (XAffineTransform.getRotation(worldToScreen) != 0.0) {
			return RendererUtilities.calculateOGCScaleAffine(
					envelope.getCoordinateReferenceSystem(),
					worldToScreen,
					hints);
		}
		return RendererUtilities.calculateOGCScale(
				envelope,
				paintArea.width,
				hints);
	}

	/**
	 * Extends the provided {@link Envelope} in order to add the number of
	 * pixels specified by <code>buffer</code> in every direction.
	 * 
	 * @param envelope
	 *            to extend.
	 * @param worldToScreen
	 *            by means of which doing the extension.
	 * @param buffer
	 *            to use for the extension.
	 * @return an extended version of the provided {@link Envelope}.
	 */
	private Envelope expandEnvelope(
			final Envelope envelope,
			final AffineTransform worldToScreen,
			final int buffer ) {
		assert buffer > 0;
		final double bufferX = Math.abs((buffer * 1.0) / XAffineTransform.getScaleX0(worldToScreen));
		final double bufferY = Math.abs((buffer * 1.0) / XAffineTransform.getScaleY0(worldToScreen));
		return new Envelope(
				envelope.getMinX() - bufferX,
				envelope.getMaxX() + bufferX,
				envelope.getMinY() - bufferY,
				envelope.getMaxY() + bufferY);
	}

	/**
	 * Queries a given layer's features to be rendered based on the target
	 * rendering bounding box.
	 * <p>
	 * The following optimization will be performed in order to limit the number
	 * of features returned:
	 * <ul>
	 * <li>Just the features whose geometric attributes lies within
	 * <code>envelope</code> will be queried</li>
	 * <li>The queried attributes will be limited to just those needed to
	 * perform the rendering, based on the required geometric and non geometric
	 * attributes found in the Layer's style rules</li>
	 * <li>If a <code>Query</code> has been set to limit the resulting layer's
	 * features, the final filter to obtain them will respect it. This means
	 * that the bounding box filter and the Query filter will be combined, also
	 * including maxFeatures from Query</li>
	 * <li>At least that the layer's definition query explicitly says to
	 * retrieve some attribute, no attributes will be requested from it, for
	 * performance reasons. So it is desirable to not use a Query for filtering
	 * a layer which includes attributes. Note that including the attributes in
	 * the result is not necessary for the query's filter to get properly
	 * processed.</li>
	 * </ul>
	 * </p>
	 * <p>
	 * <b>NOTE </b>: This is an internal method and should only be called by
	 * <code>paint(Graphics2D, Rectangle, AffineTransform)</code>. It is package
	 * protected just to allow unit testing it.
	 * </p>
	 * 
	 * @param schema
	 * @param source
	 * @param envelope
	 *            the spatial extent which is the target area of the rendering
	 *            process
	 * @param destinationCRS
	 *            DOCUMENT ME!
	 * @param sourceCrs
	 * @param screenSize
	 * @param geometryAttribute
	 * @return the set of features resulting from <code>currLayer</code> after
	 *         querying its feature source
	 * @throws IllegalFilterException
	 *             if something goes wrong constructing the bbox filter
	 * @throws IOException
	 * @see MapLayer#setQuery(org.geotools.data.Query)
	 */
	/*
	 * Default visibility for testing purposes
	 */

	Query getStyleQuery(
			final FeatureSource<FeatureType, Feature> source,
			final FeatureType schema,
			final List<LiteFeatureTypeStyle> styleList,
			Envelope mapArea,
			final CoordinateReferenceSystem mapCRS,
			final CoordinateReferenceSystem featCrs,
			final Rectangle screenSize,
			final GeometryDescriptor geometryAttribute,
			final AffineTransform worldToScreenTransform,
			final boolean renderingTransformation )
			throws IllegalFilterException,
			IOException,
			FactoryException {
		Query query = new Query(
				Query.ALL);
		Filter filter = null;

		final LiteFeatureTypeStyle[] styles = styleList.toArray(new LiteFeatureTypeStyle[styleList.size()]);

		// if map extent are not already expanded by a constant buffer, try to
		// compute a layer
		// specific one based on stroke widths
		if (getRenderingBuffer() == 0) {
			metaBuffer = findRenderingBuffer(styles);
			if (metaBuffer > 0) {
				mapArea = expandEnvelope(
						mapArea,
						worldToScreenTransform,
						metaBuffer);
				LOGGER.fine("Expanding rendering area by " + metaBuffer + " pixels to consider stroke width");

				// expand the screenmaps by the meta buffer, otherwise we'll
				// throw away geomtries
				// that sit outside of the map, but whose symbolizer may
				// contribute to it
				for (final LiteFeatureTypeStyle lfts : styles) {
					if (lfts.screenMap != null) {
						lfts.screenMap = new ScreenMap(
								lfts.screenMap,
								metaBuffer);
					}
				}
			}
		}

		// take care of rendering transforms
		mapArea = expandEnvelopeByTransformations(
				styles,
				new ReferencedEnvelope(
						mapArea,
						mapCRS));

		// build a list of attributes used in the rendering
		List<PropertyName> attributes;
		if (styles == null) {
			attributes = null;
		}
		else {
			attributes = findStyleAttributes(
					styles,
					schema);
		}

		final ReferencedEnvelope envelope = new ReferencedEnvelope(
				mapArea,
				mapCRS);
		// see what attributes we really need by exploring the styles
		// for testing purposes we have a null case -->
		try {
			// Then create the geometry filters. We have to create one for
			// each geometric attribute used during the rendering as the
			// feature may have more than one and the styles could use non
			// default geometric ones
			List<ReferencedEnvelope> envelopes;
			if (projectionHandler != null) {
				// update the envelope with the one eventually grown by the
				// rendering buffer
				projectionHandler.setRenderingEnvelope(envelope);
				envelopes = projectionHandler.getQueryEnvelopes(featCrs);
			}
			else {
				if ((mapCRS != null) && (featCrs != null) && !CRS.equalsIgnoreMetadata(
						featCrs,
						mapCRS)) {
					envelopes = Collections.singletonList(envelope.transform(
							featCrs,
							true,
							10));
				}
				else {
					envelopes = Collections.singletonList(envelope);
				}
			}

			if (LOGGER.isLoggable(Level.FINE)) {
				LOGGER.fine("Querying layer " + schema.getName() + " with bbox: " + envelope);
			}
			filter = createBBoxFilters(
					schema,
					attributes,
					envelopes);

			// now build the query using only the attributes and the
			// bounding box needed
			query = new Query(
					schema.getName().getLocalPart());
			query.setFilter(filter);
			query.setProperties(attributes);
			processRuleForQuery(
					styles,
					query);
		}
		catch (final Exception e) {
			final Exception txException = new Exception(
					"Error transforming bbox",
					e);
			LOGGER.log(
					Level.SEVERE,
					"Error querying layer",
					txException);
			fireErrorEvent(txException);

			canTransform = false;
			query = new Query(
					schema.getName().getLocalPart());
			query.setProperties(attributes);
			final Envelope bounds = source.getBounds();
			if ((bounds != null) && envelope.intersects(bounds)) {
				LOGGER.log(
						Level.WARNING,
						"Got a tranform exception while trying to de-project the current " + "envelope, bboxs intersect therefore using envelope)",
						e);
				filter = null;
				filter = createBBoxFilters(
						schema,
						attributes,
						Collections.singletonList(envelope));
				query.setFilter(filter);
			}
			else {
				LOGGER.log(
						Level.WARNING,
						"Got a tranform exception while trying to de-project the current " + "envelope, falling back on full data loading (no bbox query)",
						e);
				query.setFilter(Filter.INCLUDE);
			}
			processRuleForQuery(
					styles,
					query);

		}

		// prepare hints
		// ... basic one, we want fast and compact coordinate sequences and
		// geometries optimized
		// for the collection of one item case (typical in shapefiles)
		final LiteCoordinateSequenceFactory csFactory = new LiteCoordinateSequenceFactory();
		final GeometryFactory gFactory = new PublicSimpleGeometryFactory(
				csFactory);
		final Hints hints = new Hints(
				Hints.JTS_COORDINATE_SEQUENCE_FACTORY,
				csFactory);
		hints.put(
				Hints.JTS_GEOMETRY_FACTORY,
				gFactory);
		hints.put(
				Hints.FEATURE_2D,
				Boolean.TRUE);

		// update the screenmaps
		try {
			final CoordinateReferenceSystem crs = getNativeCRS(
					schema,
					attributes);
			if (crs != null) {
				final Set<RenderingHints.Key> fsHints = source.getSupportedHints();

				final SingleCRS crs2D = crs == null ? null : CRS.getHorizontalCRS(crs);
				final MathTransform mt = RenderUtils.buildFullTransform(
						crs2D,
						mapCRS,
						worldToScreenTransform);
				final double[] spans = Decimator.computeGeneralizationDistances(
						mt.inverse(),
						screenSize,
						generalizationDistance);
				final double distance = spans[0] < spans[1] ? spans[0] : spans[1];
				for (final LiteFeatureTypeStyle fts : styles) {
					if (fts.screenMap != null) {
						fts.screenMap.setTransform(mt);
						fts.screenMap.setSpans(
								spans[0],
								spans[1]);
						if (fsHints.contains(Hints.SCREENMAP)) {
							// replace the renderer screenmap with the hint, and
							// avoid doing
							// the work twice
							hints.put(
									Hints.SCREENMAP,
									fts.screenMap);
							fts.screenMap = null;
						}
					}
				}

				if (renderingTransformation) {
					// the RT might need valid geometries, we can at most apply
					// a topology
					// preserving generalization
					if (fsHints.contains(Hints.GEOMETRY_GENERALIZATION)) {
						hints.put(
								Hints.GEOMETRY_GENERALIZATION,
								distance);
					}
				}
				else {
					// ... if possible we let the datastore do the
					// generalization
					if (fsHints.contains(Hints.GEOMETRY_SIMPLIFICATION)) {
						// good, we don't need to perform in memory
						// generalization, the datastore
						// does it all for us
						hints.put(
								Hints.GEOMETRY_SIMPLIFICATION,
								distance);
					}
					else if (fsHints.contains(Hints.GEOMETRY_DISTANCE)) {
						// in this case the datastore can get us close, but we
						// can still
						// perform some in memory generalization
						hints.put(
								Hints.GEOMETRY_DISTANCE,
								distance);
					}
				}
			}
		}
		catch (final Exception e) {
			LOGGER.log(
					Level.INFO,
					"Error computing the generalization hints",
					e);
		}

		if (query.getHints() == null) {
			query.setHints(hints);
		}
		else {
			query.getHints().putAll(
					hints);
		}

		// simplify the filter
		final SimplifyingFilterVisitor simplifier = new SimplifyingFilterVisitor();
		final Filter simplifiedFilter = (Filter) query.getFilter().accept(
				simplifier,
				null);
		query.setFilter(simplifiedFilter);

		return query;
	}

	Query getDefinitionQuery(
			final MapLayer currLayer,
			final FeatureSource<FeatureType, Feature> source,
			final CoordinateReferenceSystem featCrs )
			throws FactoryException {
		// now, if a definition query has been established for this layer, be
		// sure to respect it by combining it with the bounding box one.
		final Query definitionQuery = reprojectQuery(
				currLayer.getQuery(),
				source);
		definitionQuery.setCoordinateSystem(featCrs);

		return definitionQuery;
	}

	/**
	 * Takes care of eventual geometric transformations
	 * 
	 * @param styles
	 * @param envelope
	 * @return
	 */
	ReferencedEnvelope expandEnvelopeByTransformations(
			final LiteFeatureTypeStyle[] styles,
			final ReferencedEnvelope envelope ) {
		final GeometryTransformationVisitor visitor = new GeometryTransformationVisitor();
		final ReferencedEnvelope result = new ReferencedEnvelope(
				envelope);
		for (final LiteFeatureTypeStyle lts : styles) {
			final List<Rule> rules = new ArrayList<Rule>();
			rules.addAll(Arrays.asList(lts.ruleList));
			rules.addAll(Arrays.asList(lts.elseRules));
			for (final Rule r : rules) {
				for (final Symbolizer s : r.symbolizers()) {
					if (s.getGeometry() != null) {
						final ReferencedEnvelope re = (ReferencedEnvelope) s.getGeometry().accept(
								visitor,
								envelope);
						if (re != null) {
							result.expandToInclude(re);
						}
					}
				}
			}
		}

		return result;
	}

	/**
	 * Scans the schema for the specified attributes are returns a single CRS if
	 * all the geometric attributes in the lot share one CRS, null if there are
	 * different ones
	 * 
	 * @param schema
	 * @return
	 */
	private CoordinateReferenceSystem getNativeCRS(
			final FeatureType schema,
			final List<PropertyName> attNames ) {
		// first off, check how many crs we have, this hint works only
		// if we have just one native CRS at hand (and the native CRS is known
		CoordinateReferenceSystem crs = null;
		// NC - property (namespace) support
		for (final PropertyName name : attNames) {
			final Object att = name.evaluate(schema);

			if (att instanceof GeometryDescriptor) {
				final GeometryDescriptor gd = (GeometryDescriptor) att;
				final CoordinateReferenceSystem gdCrs = gd.getCoordinateReferenceSystem();
				if (crs == null) {
					crs = gdCrs;
				}
				else if (gdCrs == null) {
					crs = null;
					break;
				}
				else if (!CRS.equalsIgnoreMetadata(
						crs,
						gdCrs)) {
					crs = null;
					break;
				}
			}
		}
		return crs;
	}

	/**
	 * JE: If there is a single rule "and" its filter together with the query's
	 * filter and send it off to datastore. This will allow as more processing
	 * to be done on the back end... Very useful if DataStore is a database.
	 * Problem is that worst case each filter is ran twice. Next we will modify
	 * it to find a "Common" filter between all rules and send that to the
	 * datastore.
	 * 
	 * DJB: trying to be smarter. If there are no "elseRules" and no rules w/o a
	 * filter, then it makes sense to send them off to the Datastore We limit
	 * the number of Filters sent off to the datastore, just because it could
	 * get a bit rediculous. In general, for a database, if you can limit 10% of
	 * the rows being returned you're probably doing quite well. The main
	 * problem is when your filters really mean you're secretly asking for all
	 * the data in which case sending the filters to the Datastore actually
	 * costs you. But, databases are *much* faster at processing the Filters
	 * than JAVA is and can use statistical analysis to do it.
	 * 
	 * @param styles
	 * @param q
	 */

	private void processRuleForQuery(
			final LiteFeatureTypeStyle[] styles,
			final Query q ) {
		try {

			// first we check to see if there are >
			// "getMaxFiltersToSendToDatastore" rules
			// if so, then we dont do anything since no matter what there's too
			// many to send down.
			// next we check for any else rules. If we find any --> dont send
			// anything to Datastore
			// next we check for rules w/o filters. If we find any --> dont send
			// anything to Datastore
			//
			// otherwise, we're gold and can "or" together all the filters then
			// AND it with the original filter.
			// ie. SELECT * FROM ... WHERE (the_geom && BBOX) AND (filter1 OR
			// filter2 OR filter3);

			final int maxFilters = getMaxFiltersToSendToDatastore();
			final List<Filter> filtersToDS = new ArrayList<Filter>();
			// look at each featuretypestyle
			for (final LiteFeatureTypeStyle style : styles) {
				if (style.elseRules.length > 0) {
					return;
				}
				// look at each rule in the featuretypestyle
				for (final Rule r : style.ruleList) {
					if (r.getFilter() == null) {
						return; // uh-oh has no filter
					}
					// (want all rows)
					filtersToDS.add(r.getFilter());
				}
			}

			// if too many bail out
			if (filtersToDS.size() > maxFilters) {
				return;
			}

			// or together all the filters
			org.opengis.filter.Filter ruleFiltersCombined;
			if (filtersToDS.size() == 1) {
				ruleFiltersCombined = filtersToDS.get(0);
			}
			else {
				ruleFiltersCombined = filterFactory.or(filtersToDS);
			}

			// combine with the pre-existing filter
			ruleFiltersCombined = filterFactory.and(
					q.getFilter(),
					ruleFiltersCombined);
			q.setFilter(ruleFiltersCombined);
		}
		catch (final Exception e) {
			if (LOGGER.isLoggable(Level.WARNING)) {
				LOGGER.log(
						Level.SEVERE,
						"Could not send rules to datastore due to: " + e.getLocalizedMessage(),
						e);
			}
		}
	}

	/**
	 * find out the maximum number of filters we're going to send off to the
	 * datastore. See processRuleForQuery() for details.
	 * 
	 */
	private int getMaxFiltersToSendToDatastore() {
		try {
			if (rendererHints == null) {
				return defaultMaxFiltersToSendToDatastore;
			}

			final Integer result = (Integer) rendererHints.get("maxFiltersToSendToDatastore");
			if (result == null) {
				return defaultMaxFiltersToSendToDatastore; // default
			}
			// if
			// not
			// present in hints
			return result.intValue();

		}
		catch (final Exception e) {
			return defaultMaxFiltersToSendToDatastore;
		}
	}

	/**
	 * Checks if optimized feature type style rendering is enabled, or not. See
	 * {@link #OPTIMIZE_FTS_RENDERING_KEY} description for a full explanation.
	 */
	private boolean isOptimizedFTSRenderingEnabled() {
		if (rendererHints == null) {
			return true;
		}
		final Object result = rendererHints.get(OPTIMIZE_FTS_RENDERING_KEY);
		if (result == null) {
			return true;
		}
		return Boolean.TRUE.equals(result);
	}

	/**
	 * Checks if the advanced projection handling is enabled
	 * 
	 * @return
	 */
	private boolean isAdvancedProjectionHandlingEnabled() {
		if (rendererHints == null) {
			return false;
		}
		final Object result = rendererHints.get(ADVANCED_PROJECTION_HANDLING_KEY);
		if (result == null) {
			return false;
		}
		return Boolean.TRUE.equals(result);
	}

	/**
	 * Checks if continuous map wrapping is enabled
	 * 
	 * @return
	 */
	private boolean isMapWrappingEnabled() {
		if (rendererHints == null) {
			return false;
		}
		final Object result = rendererHints.get(CONTINUOUS_MAP_WRAPPING);
		if (result == null) {
			return false;
		}
		return Boolean.TRUE.equals(result);
	}

	/**
	 * Checks if the geometries in spatial filters in the SLD must be assumed to
	 * be expressed in the official EPSG axis order, regardless of how the
	 * referencing subsystem is configured (this is required to support filter
	 * reprojection in WMS 1.3+)
	 * 
	 * @return
	 */
	private boolean isEPSGAxisOrderForced() {
		if (rendererHints == null) {
			return false;
		}
		final Object result = rendererHints.get(FORCE_EPSG_AXIS_ORDER_KEY);
		if (result == null) {
			return false;
		}
		return Boolean.TRUE.equals(result);
	}

	/**
	 * Checks if vector rendering is enabled or not. See
	 * {@link SLDStyleFactory#isVectorRenderingEnabled()} for a full
	 * explanation.
	 */
	private boolean isVectorRenderingEnabled() {
		if (rendererHints == null) {
			return true;
		}
		final Object result = rendererHints.get(VECTOR_RENDERING_KEY);
		if (result == null) {
			return VECTOR_RENDERING_ENABLED_DEFAULT;
		}
		return ((Boolean) result).booleanValue();
	}

	/**
	 * Returns an estimate of the rendering buffer needed to properly display
	 * this layer taking into consideration the constant stroke sizes in the
	 * feature type styles.
	 * 
	 * @param styles
	 *            the feature type styles to be applied to the layer
	 * @return an estimate of the buffer that should be used to properly display
	 *         a layer rendered with the specified styles
	 */
	private int findRenderingBuffer(
			final LiteFeatureTypeStyle[] styles ) {
		final MetaBufferEstimator rbe = new MetaBufferEstimator();

		for (final LiteFeatureTypeStyle lfts : styles) {
			Rule[] rules = lfts.elseRules;
			for (final Rule rule : rules) {
				rbe.visit(rule);
			}
			rules = lfts.ruleList;
			for (final Rule rule : rules) {
				rbe.visit(rule);
			}
		}

		if (!rbe.isEstimateAccurate()) {
			LOGGER.fine("Assuming rendering buffer = " + rbe.getBuffer() + ", but estimation is not accurate, you may want to set a buffer manually");
		}

		// the actual amount we have to grow the rendering area by is half of
		// the stroke/symbol sizes
		// plus one extra pixel for antialiasing effects
		return (int) Math.round((rbe.getBuffer() / 2.0) + 1);
	}

	/**
	 * Inspects the <code>MapLayer</code>'s style and retrieves it's needed
	 * attribute names, returning at least the default geometry attribute name.
	 * 
	 * @param layer
	 *            the <code>MapLayer</code> to determine the needed attributes
	 *            from
	 * @param schema
	 *            the <code>layer</code>'s FeatureSource<SimpleFeatureType,
	 *            SimpleFeature> schema
	 * @return the minimum set of attribute names needed to render
	 *         <code>layer</code>
	 */
	private List<PropertyName> findStyleAttributes(
			final LiteFeatureTypeStyle[] styles,
			final FeatureType schema ) {
		final StyleAttributeExtractor sae = new StyleAttributeExtractor();

		LiteFeatureTypeStyle lfts;
		Rule[] rules;
		int rulesLength;
		final int length = styles.length;
		for (int t = 0; t < length; t++) {
			lfts = styles[t];
			rules = lfts.elseRules;
			rulesLength = rules.length;
			for (int j = 0; j < rulesLength; j++) {
				sae.visit(rules[j]);
			}
			rules = lfts.ruleList;
			rulesLength = rules.length;
			for (int j = 0; j < rulesLength; j++) {
				sae.visit(rules[j]);
			}
		}

		if (sae.isUsingDynamincProperties()) {
			return null;
		}
		final Set<PropertyName> attributes = sae.getAttributes();
		final Set<String> attributeNames = sae.getAttributeNameSet();

		/*
		 * DJB: this is an old comment - erase it soon (see geos-469 and below)
		 * - we only add the default geometry if it was used.
		 * 
		 * GR: if as result of sae.getAttributeNames() ftsAttributes already
		 * contains geometry attribute names, they gets duplicated, which
		 * produces an error in AbstracDatastore when trying to create a
		 * derivate SimpleFeatureType. So I'll add the default geometry only if
		 * it is not already present, but: should all the geometric attributes
		 * be added by default? I will add them, but don't really know what's
		 * the expected behavior
		 */
		final List<PropertyName> atts = new ArrayList<PropertyName>(
				attributes);
		final Collection<PropertyDescriptor> attTypes = schema.getDescriptors();
		Name attName;

		for (final PropertyDescriptor pd : attTypes) {
			// attName = pd.getName().getLocalPart();
			attName = pd.getName();

			// DJB: This geometry check was commented out. I think it should
			// actually be back in or
			// you get ALL the attributes back, which isn't what you want.
			// ALX: For rasters I need even the "grid" attribute.

			// DJB:geos-469, we do not grab all the geometry columns.
			// for symbolizers, if a geometry is required it is either
			// explicitly named
			// ("<Geometry><PropertyName>the_geom</PropertyName></Geometry>")
			// or the default geometry is assumed (no <Geometry> element).
			// I've modified the style attribute extractor so it tracks if the
			// default geometry is used. So, we no longer add EVERY geometry
			// column to the query!!

			if (((attName.getLocalPart().equalsIgnoreCase("grid")) && !attributeNames.contains(attName.getLocalPart())) || ((attName.getLocalPart().equalsIgnoreCase("params")) && !attributeNames.contains(attName.getLocalPart()))) {
				atts.add(filterFactory.property(attName));
				if (LOGGER.isLoggable(Level.FINE)) {
					LOGGER.fine("added attribute " + attName);
				}
			}
		}

		try {
			// DJB:geos-469 if the default geometry was used in the style, we
			// need to grab it.
			if (sae.getDefaultGeometryUsed() && (!attributeNames.contains(schema.getGeometryDescriptor().getName().toString()))) {
				atts.add(filterFactory.property(schema.getGeometryDescriptor().getName()));
			}
		}
		catch (final Exception e) {
			// might not be a geometry column. That will cause problems down the
			// road (why render a non-geometry layer)
		}

		return atts;
	}

	/**
	 * Creates the bounding box filters (one for each geometric attribute)
	 * needed to query a <code>MapLayer</code>'s feature source to return just
	 * the features for the target rendering extent
	 * 
	 * @param schema
	 *            the layer's feature source schema
	 * @param attributes
	 *            set of needed attributes
	 * @param bbox
	 *            the expression holding the target rendering bounding box
	 * @return an or'ed list of bbox filters, one for each geometric attribute
	 *         in <code>attributes</code>. If there are just one geometric
	 *         attribute, just returns its corresponding
	 *         <code>GeometryFilter</code>.
	 * @throws IllegalFilterException
	 *             if something goes wrong creating the filter
	 */
	private Filter createBBoxFilters(
			final FeatureType schema,
			final List<PropertyName> attributes,
			final List<ReferencedEnvelope> bboxes )
			throws IllegalFilterException {
		Filter filter = Filter.INCLUDE;
		final int length = attributes.size();
		Object attType;

		for (int j = 0; j < length; j++) {
			// NC - support nested attributes -> use evaluation for getting
			// descriptor
			// result is not necessary a descriptor, is Name in case of
			// @attribute
			attType = attributes.get(
					j).evaluate(
					schema);

			// the attribute type might be missing because of rendering
			// transformations, skip it
			if (attType == null) {
				continue;
			}

			if (attType instanceof GeometryDescriptor) {
				final Filter gfilter = new PublicFastBBOX(
						attributes.get(j),
						bboxes.get(0),
						filterFactory);

				if (filter == Filter.INCLUDE) {
					filter = gfilter;
				}
				else {
					filter = filterFactory.or(
							filter,
							gfilter);
				}

				if (bboxes.size() > 0) {
					for (int k = 1; k < bboxes.size(); k++) {
						// filter = filterFactory.or( filter, new
						// FastBBOX(localName, bboxes.get(k), filterFactory) );
						filter = filterFactory.or(
								filter,
								new PublicFastBBOX(
										attributes.get(j),
										bboxes.get(k),
										filterFactory));
					}
				}
			}
		}

		return filter;
	}

	/**
	 * Checks if a rule can be triggered at the current scale level
	 * 
	 * @param r
	 *            The rule
	 * @return true if the scale is compatible with the rule settings
	 */
	private boolean isWithInScale(
			final Rule r ) {
		return ((r.getMinScaleDenominator() - TOLERANCE) <= scaleDenominator) && ((r.getMaxScaleDenominator() + TOLERANCE) > scaleDenominator);
	}

	/**
	 * <p>
	 * Creates a list of <code>LiteFeatureTypeStyle</code>s with:
	 * <ol type="a">
	 * <li>out-of-scale rules removed</li>
	 * <li>incompatible FeatureTypeStyles removed</li>
	 * </ol>
	 * </p>
	 * 
	 * <p>
	 * <em><strong>Note:</strong> This method has a lot of duplication with 
	 * {@link #createLiteFeatureTypeStyles(FeatureTypeStyle[], SimpleFeatureType, Graphics2D)}. 
	 * </em>
	 * </p>
	 * 
	 * @param featureStyles
	 *            Styles to process
	 * @param typeDescription
	 *            The type description that has to be matched
	 * @return ArrayList<LiteFeatureTypeStyle>
	 */
	private ArrayList<LiteFeatureTypeStyle> createLiteFeatureTypeStyles(
			final List<FeatureTypeStyle> featureStyles,
			final Object typeDescription,
			final Graphics2D graphics )
			throws IOException {
		final ArrayList<LiteFeatureTypeStyle> result = new ArrayList<LiteFeatureTypeStyle>();

		List<Rule> rules;
		List<Rule> ruleList;
		List<Rule> elseRuleList;
		LiteFeatureTypeStyle lfts;
		final BufferedImage image;

		for (final FeatureTypeStyle fts : featureStyles) {
			if ((typeDescription == null) || (typeDescription.toString().indexOf(
					fts.getFeatureTypeName()) == -1)) {
				continue;
			}

			// get applicable rules at the current scale
			rules = fts.rules();
			ruleList = new ArrayList<Rule>();
			elseRuleList = new ArrayList<Rule>();

			// gather the active rules
			for (final Rule r : rules) {
				if (isWithInScale(r)) {
					if (r.isElseFilter()) {
						elseRuleList.add(r);
					}
					else {
						ruleList.add(r);
					}
				}
			}

			// nothing to render, don't do anything!!
			if ((ruleList.isEmpty()) && (elseRuleList.isEmpty())) {
				continue;
			}

			// first fts, we can reuse the graphics directly
			if (result.isEmpty() || !isOptimizedFTSRenderingEnabled()) {
				lfts = new LiteFeatureTypeStyle(
						ruleList,
						elseRuleList,
						fts.getTransformation());
			}
			else {
				lfts = new LiteFeatureTypeStyle(
						ruleList,
						elseRuleList,
						fts.getTransformation());
			}
			result.add(lfts);
		}

		return result;
	}

	/**
	 * creates a list of LiteFeatureTypeStyles a) out-of-scale rules removed b)
	 * incompatible FeatureTypeStyles removed
	 * 
	 * 
	 * @param featureStylers
	 * @param features
	 * @throws Exception
	 * @return ArrayList<LiteFeatureTypeStyle>
	 */
	private ArrayList<LiteFeatureTypeStyle> createLiteFeatureTypeStyles(
			final List<FeatureTypeStyle> featureStyles,
			final FeatureType ftype,
			final Graphics2D graphics )
			throws IOException {
		if (LOGGER.isLoggable(Level.FINE)) {
			LOGGER.fine("creating rules for scale denominator - " + NumberFormat.getNumberInstance().format(
					scaleDenominator));
		}
		final ArrayList<LiteFeatureTypeStyle> result = new ArrayList<LiteFeatureTypeStyle>();

		LiteFeatureTypeStyle lfts;
		for (final FeatureTypeStyle fts : featureStyles) {
			if (isFeatureTypeStyleActive(
					ftype,
					fts)) {
				// DJB: this FTS is compatible with this FT.

				// get applicable rules at the current scale
				final List[] splittedRules = splitRules(fts);
				final List ruleList = splittedRules[0];
				final List elseRuleList = splittedRules[1];

				// if none, skip it
				if ((ruleList.isEmpty()) && (elseRuleList.isEmpty())) {
					continue;
				}

				// we can optimize this one!
				if (result.isEmpty() || !isOptimizedFTSRenderingEnabled()) {
					lfts = new LiteFeatureTypeStyle(
							ruleList,
							elseRuleList,
							fts.getTransformation());
				}
				else {
					lfts = new LiteFeatureTypeStyle(
							ruleList,
							elseRuleList,
							fts.getTransformation());
				}
				if (screenMapEnabled(lfts)) {
					lfts.screenMap = new ScreenMap(
							screenSize.x,
							screenSize.y,
							screenSize.width,
							screenSize.height);
				}

				result.add(lfts);
			}
		}

		return result;
	}

	/**
	 * Returns true if the ScreenMap optimization can be applied given the
	 * current renderer and configuration and the style to be applied
	 * 
	 * @param lfts
	 * @return
	 */
	boolean screenMapEnabled(
			final LiteFeatureTypeStyle lfts ) {
		if (generalizationDistance == 0.0) {
			return false;
		}

		final OpacityFinder finder = new OpacityFinder(
				new Class[] {
					PointSymbolizer.class,
					LineSymbolizer.class,
					PolygonSymbolizer.class
				});
		for (final Rule r : lfts.ruleList) {
			r.accept(finder);
		}
		for (final Rule r : lfts.elseRules) {
			r.accept(finder);
		}

		return !finder.hasOpacity;
	}

	private boolean isFeatureTypeStyleActive(
			final FeatureType ftype,
			final FeatureTypeStyle fts ) {
		// TODO: find a complex feature equivalent for this check
		return fts.featureTypeNames().isEmpty() || ((ftype.getName().getLocalPart() != null) && (ftype.getName().getLocalPart().equalsIgnoreCase(
				fts.getFeatureTypeName()) || FeatureTypes.isDecendedFrom(
				ftype,
				null,
				fts.getFeatureTypeName())));
	}

	private List[] splitRules(
			final FeatureTypeStyle fts ) {
		Rule[] rules;
		List<Rule> ruleList = new ArrayList<Rule>();
		List<Rule> elseRuleList = new ArrayList<Rule>();

		rules = fts.getRules();
		ruleList = new ArrayList();
		elseRuleList = new ArrayList();

		for (final Rule r : rules) {
			if (isWithInScale(r)) {
				if (r.isElseFilter()) {
					elseRuleList.add(r);
				}
				else {
					ruleList.add(r);
				}
			}
		}

		return new List[] {
			ruleList,
			elseRuleList
		};
	}

	/**
	 * Makes sure the feature collection generates the desired sourceCrs, this
	 * is mostly a workaround against feature sources generating feature
	 * collections without a CRS (which is fatal to the reprojection handling
	 * later in the code)
	 * 
	 * @param features
	 * @param sourceCrs
	 * @return FeatureCollection<SimpleFeatureType, SimpleFeature> that produces
	 *         results with the correct CRS
	 */
	private FeatureCollection prepFeatureCollection(
			final FeatureCollection features,
			final CoordinateReferenceSystem sourceCrs ) {
		// this is the reader's CRS
		CoordinateReferenceSystem rCS = null;
		try {
			rCS = features.getSchema().getGeometryDescriptor().getType().getCoordinateReferenceSystem();
		}
		catch (final NullPointerException e) {
			// life sucks sometimes
		}

		if ((rCS != sourceCrs) && (sourceCrs != null)) {
			// if the datastore is producing null CRS, we recode.
			// if the datastore's CRS != real CRS, then we recode
			if ((rCS == null) || !CRS.equalsIgnoreMetadata(
					rCS,
					sourceCrs)) {
				// need to retag the features
				try {
					return new ForceCoordinateSystemFeatureResults(
							features,
							sourceCrs);
				}
				catch (final Exception ee) {
					LOGGER.log(
							Level.WARNING,
							ee.getLocalizedMessage(),
							ee);
				}
			}
		}
		return features;
	}

	/**
	 * Classify a List of LiteFeatureTypeStyle objects by Transformation.
	 * 
	 * @param lfts
	 *            A List of LiteFeatureTypeStyles
	 * @return A List of List of LiteFeatureTypeStyles
	 */
	List<List<LiteFeatureTypeStyle>> classifyByTransformation(
			final List<LiteFeatureTypeStyle> lfts ) {
		final List<List<LiteFeatureTypeStyle>> txClassified = new ArrayList<List<LiteFeatureTypeStyle>>();
		txClassified.add(new ArrayList<LiteFeatureTypeStyle>());
		Expression transformation = null;
		for (int i = 0; i < lfts.size(); i++) {
			final LiteFeatureTypeStyle curr = lfts.get(i);
			if (i == 0) {
				transformation = curr.transformation;
			}
			else if (!(transformation == curr.transformation) || ((transformation != null) && (curr.transformation != null) && !curr.transformation.equals(transformation))) {
				txClassified.add(new ArrayList<LiteFeatureTypeStyle>());

			}
			txClassified.get(
					txClassified.size() - 1).add(
					curr);
		}
		return txClassified;
	}

	/**
	 * Checks the attributes in the query (which we got from the SLD) match the
	 * schema, throws an {@link IllegalFilterException} otherwise
	 * 
	 * @param schema
	 * @param attributeNames
	 */
	void checkAttributeExistence(
			final FeatureType schema,
			final Query query ) {
		if (query.getProperties() == null) {
			return;
		}

		for (final PropertyName attribute : query.getProperties()) {
			if (attribute.evaluate(schema) == null) {
				if (schema instanceof SimpleFeatureType) {
					final List<Name> allNames = new ArrayList<Name>();
					for (final PropertyDescriptor pd : schema.getDescriptors()) {
						allNames.add(pd.getName());
					}
					throw new IllegalFilterException(
							"Could not find '" + attribute + "' in the FeatureType (" + schema.getName() + "), available attributes are: " + allNames);
				}
				else {
					throw new IllegalFilterException(
							"Could not find '" + attribute + "' in the FeatureType (" + schema.getName() + ")");
				}
			}
		}
	}

	FeatureCollection applyRenderingTransformation(
			final Expression transformation,
			final FeatureSource featureSource,
			final Query layerQuery,
			final Query renderingQuery,
			final GridGeometry2D gridGeometry,
			final CoordinateReferenceSystem sourceCrs )
			throws IOException,
			SchemaException,
			TransformException,
			FactoryException {
		Object result = null;

		// check if it's a wrapper coverage or a wrapped reader
		final FeatureType schema = featureSource.getSchema();
		boolean isRasterData = false;
		if (schema instanceof SimpleFeatureType) {
			final SimpleFeatureType simpleSchema = (SimpleFeatureType) schema;
			GridCoverage2D coverage = null;
			if (FeatureUtilities.isWrappedCoverage(simpleSchema) || FeatureUtilities.isWrappedCoverageReader(simpleSchema)) {
				isRasterData = true;

				// get the desired grid geometry
				GridGeometry2D readGG = gridGeometry;
				if (transformation instanceof RenderingTransformation) {
					final RenderingTransformation tx = (RenderingTransformation) transformation;
					readGG = (GridGeometry2D) tx.invertGridGeometry(
							renderingQuery,
							gridGeometry);
					// TODO: override the read params and force this grid
					// geometry, or something
					// similar to this (like passing it as a param to
					// readCoverage
				}

				final FeatureCollection<?, ?> sample = featureSource.getFeatures();
				final Feature gridWrapper = DataUtilities.first(sample);

				if (FeatureUtilities.isWrappedCoverageReader(simpleSchema)) {
					final Object params = paramsPropertyName.evaluate(gridWrapper);
					final GridCoverage2DReader reader = (GridCoverage2DReader) gridPropertyName.evaluate(gridWrapper);
					// don't read more than the native resolution (in case we
					// are oversampling)
					if (CRS.equalsIgnoreMetadata(
							reader.getCoordinateReferenceSystem(),
							gridGeometry.getCoordinateReferenceSystem())) {
						final MathTransform g2w = reader.getOriginalGridToWorld(PixelInCell.CELL_CENTER);
						if ((g2w instanceof AffineTransform2D) && (readGG.getGridToCRS2D() instanceof AffineTransform2D)) {
							final AffineTransform2D atOriginal = (AffineTransform2D) g2w;
							final AffineTransform2D atMap = (AffineTransform2D) readGG.getGridToCRS2D();
							if (XAffineTransform.getScale(atMap) < XAffineTransform.getScale(atOriginal)) {
								// we need to go trough some convoluted code to
								// make sure the new grid geometry
								// has at least one pixel

								final org.opengis.geometry.Envelope worldEnvelope = gridGeometry.getEnvelope();
								final GeneralEnvelope transformed = org.geotools.referencing.CRS.transform(
										atOriginal.inverse(),
										worldEnvelope);
								final int minx = (int) Math.floor(transformed.getMinimum(0));
								final int miny = (int) Math.floor(transformed.getMinimum(1));
								final int maxx = (int) Math.ceil(transformed.getMaximum(0));
								final int maxy = (int) Math.ceil(transformed.getMaximum(1));
								final Rectangle rect = new Rectangle(
										minx,
										miny,
										(maxx - minx),
										(maxy - miny));
								final GridEnvelope2D gridEnvelope = new GridEnvelope2D(
										rect);
								readGG = new GridGeometry2D(
										gridEnvelope,
										atOriginal,
										worldEnvelope.getCoordinateReferenceSystem());
							}
						}
					}
					coverage = readCoverage(
							reader,
							params,
							readGG);
				}
				else {
					coverage = (GridCoverage2D) gridPropertyName.evaluate(gridWrapper);
				}

				// readers will return null if there is no coverage in the area
				if (coverage != null) {
					if (readGG != null) {
						// Crop will fail if we try to crop outside of the
						// coverage area
						ReferencedEnvelope renderingEnvelope = new ReferencedEnvelope(
								readGG.getEnvelope());
						final CoordinateReferenceSystem coverageCRS = coverage.getCoordinateReferenceSystem2D();
						if (!CRS.equalsIgnoreMetadata(
								renderingEnvelope.getCoordinateReferenceSystem(),
								coverageCRS)) {
							renderingEnvelope = renderingEnvelope.transform(
									coverageCRS,
									true);
						}
						if (coverage.getEnvelope2D().intersects(
								renderingEnvelope)) {
							// the resulting coverage might be larger than the
							// readGG envelope, shall we crop it?
							final ParameterValueGroup param = CROP.getParameters();
							param.parameter(
									"Source").setValue(
									coverage);
							param.parameter(
									"Envelope").setValue(
									renderingEnvelope);
							coverage = (GridCoverage2D) PROCESSOR.doOperation(param);
						}
						else {
							coverage = null;
						}

						if (coverage != null) {
							// we might also need to scale the coverage to the
							// desired resolution
							final MathTransform2D coverageTx = readGG.getGridToCRS2D();
							if (coverageTx instanceof AffineTransform) {
								final AffineTransform coverageAt = (AffineTransform) coverageTx;
								final AffineTransform renderingAt = (AffineTransform) gridGeometry.getGridToCRS2D();
								// we adjust the scale only if we have many more
								// pixels than required (30% or more)
								final double ratioX = coverageAt.getScaleX() / renderingAt.getScaleX();
								final double ratioY = coverageAt.getScaleY() / renderingAt.getScaleY();
								if ((ratioX < 0.7) && (ratioY < 0.7)) {
									// resolution is too different
									final ParameterValueGroup param = SCALE.getParameters();
									param.parameter(
											"Source").setValue(
											coverage);
									param.parameter(
											"xScale").setValue(
											ratioX);
									param.parameter(
											"yScale").setValue(
											ratioY);
									final Interpolation interpolation = (Interpolation) java2dHints.get(JAI.KEY_INTERPOLATION);
									if (interpolation != null) {
										param.parameter(
												"Interpolation").setValue(
												interpolation);
									}

									coverage = (GridCoverage2D) PROCESSOR.doOperation(param);
								}
							}
						}
					}

					if (coverage != null) {
						// apply the transformation
						result = transformation.evaluate(coverage);
					}
					else {
						result = null;
					}
				}
			}
		}

		if ((result == null) && !isRasterData) {
			// it's a transformation starting from vector data, let's see if we
			// can optimize the query
			FeatureCollection originalFeatures;
			Query optimizedQuery = null;
			if (transformation instanceof RenderingTransformation) {
				final RenderingTransformation tx = (RenderingTransformation) transformation;
				optimizedQuery = tx.invertQuery(
						renderingQuery,
						gridGeometry);
			}
			// if we could not find an optimized query no other choice but to
			// just limit
			// ourselves to the bbox, we don't know if the transformation
			// alters/adds attributes :-(
			if (optimizedQuery == null) {
				final Envelope bounds = (Envelope) renderingQuery.getFilter().accept(
						ExtractBoundsFilterVisitor.BOUNDS_VISITOR,
						null);
				final Filter bbox = new PublicFastBBOX(
						filterFactory.property(""),
						bounds,
						filterFactory);
				optimizedQuery = new Query(
						null,
						bbox);
				optimizedQuery.setHints(layerQuery.getHints());
			}

			// grab the original features
			final Query mixedQuery = DataUtilities.mixQueries(
					layerQuery,
					optimizedQuery,
					null);
			originalFeatures = featureSource.getFeatures(mixedQuery);
			prepFeatureCollection(
					originalFeatures,
					sourceCrs);

			// transform them
			result = transformation.evaluate(originalFeatures);
		}

		// null safety, a transformation might be free to return null
		if (result == null) {
			return null;
		}

		// what did we get? raster or vector?
		if (result instanceof FeatureCollection) {
			return (FeatureCollection) result;
		}
		else if (result instanceof GridCoverage2D) {
			return FeatureUtilities.wrapGridCoverage((GridCoverage2D) result);
		}
		else if (result instanceof GridCoverage2DReader) {
			return FeatureUtilities.wrapGridCoverageReader(
					(GridCoverage2DReader) result,
					null);
		}
		else {
			throw new IllegalArgumentException(
					"Don't know how to handle the results of the transformation, " + "the supported result types are FeatureCollection, GridCoverage2D " + "and GridCoverage2DReader, but we got: " + result.getClass());
		}
	}

	/**
	 * Applies Unit Of Measure rescaling against all symbolizers, the result
	 * will be symbolizers that operate purely in pixels
	 * 
	 * @param lfts
	 */
	void applyUnitRescale(
			final ArrayList<LiteFeatureTypeStyle> lfts ) {
		// apply dpi rescale
		final double dpi = RendererUtilities.getDpi(getRendererHints());
		final double standardDpi = RendererUtilities.getDpi(Collections.emptyMap());
		if (dpi != standardDpi) {
			final double scaleFactor = dpi / standardDpi;
			final DpiRescaleStyleVisitor dpiVisitor = new GraphicsAwareDpiRescaleStyleVisitor(
					scaleFactor);
			for (final LiteFeatureTypeStyle fts : lfts) {
				rescaleFeatureTypeStyle(
						fts,
						dpiVisitor);
			}
		}

		// apply UOM rescaling
		final double pixelsPerMeters = RendererUtilities.calculatePixelsPerMeterRatio(
				scaleDenominator,
				rendererHints);
		final UomRescaleStyleVisitor rescaleVisitor = new UomRescaleStyleVisitor(
				pixelsPerMeters);
		for (final LiteFeatureTypeStyle fts : lfts) {
			rescaleFeatureTypeStyle(
					fts,
					rescaleVisitor);
		}
	}

	/**
	 * Reprojects the spatial filters in each {@link LiteFeatureTypeStyle} so
	 * that they match the feature source native coordinate system
	 * 
	 * @param lfts
	 * @param fs
	 * @throws FactoryException
	 */
	void reprojectSpatialFilters(
			final ArrayList<LiteFeatureTypeStyle> lfts,
			final FeatureSource fs )
			throws FactoryException {
		final FeatureType schema = fs.getSchema();
		final CoordinateReferenceSystem declaredCRS = getDeclaredSRS(schema);

		// reproject spatial filters in each fts
		for (final LiteFeatureTypeStyle fts : lfts) {
			reprojectSpatialFilters(
					fts,
					declaredCRS,
					schema);
		}
	}

	/**
	 * Computes the declared SRS of a layer based on the layer schema and the
	 * EPSG forcing flag
	 * 
	 * @param schema
	 * @return
	 * @throws FactoryException
	 * @throws NoSuchAuthorityCodeException
	 */
	private CoordinateReferenceSystem getDeclaredSRS(
			final FeatureType schema )
			throws FactoryException {
		// compute the default SRS of the feature source
		CoordinateReferenceSystem declaredCRS = schema.getCoordinateReferenceSystem();
		if (isEPSGAxisOrderForced()) {
			final Integer code = CRS.lookupEpsgCode(
					declaredCRS,
					false);
			if (code != null) {
				declaredCRS = CRS.decode("urn:ogc:def:crs:EPSG::" + code);
			}
		}
		return declaredCRS;
	}

	/**
	 * Reprojects all spatial filters in the specified Query so that they match
	 * the native srs of the specified feature source
	 * 
	 * @param query
	 * @param source
	 * @return
	 * @throws FactoryException
	 */
	private Query reprojectQuery(
			final Query query,
			final FeatureSource<FeatureType, Feature> source )
			throws FactoryException {
		if ((query == null) || (query.getFilter() == null)) {
			return query;
		}

		// compute the declared CRS
		final Filter original = query.getFilter();
		final CoordinateReferenceSystem declaredCRS = getDeclaredSRS(source.getSchema());
		final Filter reprojected = reprojectSpatialFilter(
				declaredCRS,
				source.getSchema(),
				original);
		if (reprojected == original) {
			return query;
		}
		else {
			final Query rq = new Query(
					query);
			rq.setFilter(reprojected);
			return rq;
		}
	}

	/**
	 * Reprojects spatial filters so that they match the feature source native
	 * CRS, and assuming all literal geometries are specified in the specified
	 * declaredCRS
	 */
	void reprojectSpatialFilters(
			final LiteFeatureTypeStyle fts,
			final CoordinateReferenceSystem declaredCRS,
			final FeatureType schema ) {
		for (int i = 0; i < fts.ruleList.length; i++) {
			fts.ruleList[i] = reprojectSpatialFilters(
					fts.ruleList[i],
					declaredCRS,
					schema);
		}
		if (fts.elseRules != null) {
			for (int i = 0; i < fts.elseRules.length; i++) {
				fts.elseRules[i] = reprojectSpatialFilters(
						fts.elseRules[i],
						declaredCRS,
						schema);
			}
		}
	}

	/**
	 * Reprojects spatial filters so that they match the feature source native
	 * CRS, and assuming all literal geometries are specified in the specified
	 * declaredCRS
	 */
	private Rule reprojectSpatialFilters(
			final Rule rule,
			final CoordinateReferenceSystem declaredCRS,
			final FeatureType schema ) {
		// NPE avoidance
		final Filter filter = rule.getFilter();
		if (filter == null) {
			return rule;
		}

		// try to reproject the filter
		final Filter reprojected = reprojectSpatialFilter(
				declaredCRS,
				schema,
				filter);
		if (reprojected == filter) {
			return rule;
		}

		// clone the rule (the style can be reused over and over, we cannot
		// alter it) and set the new filter
		final Rule rr = new RuleImpl(
				rule);
		rr.setFilter(reprojected);
		return rr;
	}

	/**
	 * Reprojects spatial filters so that they match the feature source native
	 * CRS, and assuming all literal geometries are specified in the specified
	 * declaredCRS
	 */
	private Filter reprojectSpatialFilter(
			final CoordinateReferenceSystem declaredCRS,
			final FeatureType schema,
			final Filter filter ) {
		// NPE avoidance
		if (filter == null) {
			return null;
		}

		// do we have any spatial filter?
		final SpatialFilterVisitor sfv = new SpatialFilterVisitor();
		filter.accept(
				sfv,
				null);
		if (!sfv.hasSpatialFilter()) {
			return filter;
		}

		// all right, we need to default the literals to the declaredCRS and
		// then reproject to
		// the native one
		final DefaultCRSFilterVisitor defaulter = new DefaultCRSFilterVisitor(
				filterFactory,
				declaredCRS);
		final Filter defaulted = (Filter) filter.accept(
				defaulter,
				null);
		final ReprojectingFilterVisitor reprojector = new ReprojectingFilterVisitor(
				filterFactory,
				schema);
		final Filter reprojected = (Filter) defaulted.accept(
				reprojector,
				null);
		return reprojected;
	}

	/**
	 * Utility method to apply the two rescale visitors without duplicating code
	 * 
	 * @param fts
	 * @param visitor
	 */
	void rescaleFeatureTypeStyle(
			final LiteFeatureTypeStyle fts,
			final DuplicatingStyleVisitor visitor ) {
		for (int i = 0; i < fts.ruleList.length; i++) {
			visitor.visit(fts.ruleList[i]);
			fts.ruleList[i] = (Rule) visitor.getCopy();
		}
		if (fts.elseRules != null) {
			for (int i = 0; i < fts.elseRules.length; i++) {
				visitor.visit(fts.elseRules[i]);
				fts.elseRules[i] = (Rule) visitor.getCopy();
			}
		}
	}

	private ServerFeatureRenderer getServerFeatureRenderer(
			final List<LiteFeatureTypeStyle> lfts,
			final Rectangle paintRect,
			final ReferencedEnvelope env,
			final double angle,
			final MapLayer currLayer,
			final RenderingHints renderingHints,
			final Color bgColor,
			final int metaBuffer,
			final double scaleDenominator,
			final boolean useAlpha,
			final LabelRenderingMode labelRenderingMode,
			final ServerDecimationOptions decimationOptions )
			throws TransformException,
			FactoryException {
		final ServerMapArea mapArea = new ServerMapArea(
				env);
		final ServerPaintArea paintArea = new ServerPaintArea(
				(int) paintRect.getWidth(),
				(int) paintRect.getHeight());
		final List<ServerFeatureStyle> styles = new ArrayList<ServerFeatureStyle>(
				lfts.size());
		int i = 0;
		for (final LiteFeatureTypeStyle fts : lfts) {
			styles.add(new ServerFeatureStyle(
					Integer.toString(i++),
					fts.ruleList,
					fts.elseRules,
					fts.screenMap));
		}
		final boolean clone = isCloningRequired(
				currLayer,
				lfts);
		final ServerRenderOptions renderOptions = new ServerRenderOptions(
				renderingHints,
				bgColor,
				labelRenderingMode,
				metaBuffer,
				scaleDenominator,
				angle,
				useAlpha,
				isMapWrappingEnabled(),
				isAdvancedProjectionHandlingEnabled(),
				clone,
				isVectorRenderingEnabled(),
				isLineWidthOptimizationEnabled());

		return new ServerFeatureRenderer(
				paintArea,
				mapArea,
				styles,
				renderOptions,
				decimationOptions);
	}

	/**
	 * Tells if geometry cloning is required or not
	 */
	private boolean isCloningRequired(
			final MapLayer layer,
			final List<LiteFeatureTypeStyle> lfts ) {
		// check if the features are detached, we can thus modify the geometries
		// in place
		final Set<Key> hints = layer.getFeatureSource().getSupportedHints();
		if (!hints.contains(Hints.FEATURE_DETACHED)) {
			return true;
		}

		// check if there is any conflicting geometry transformation.
		// No geometry transformations -> we can modify geometries in place
		// Just one geometry transformation over an attribute -> we can modify
		// geometries in place
		// Two tx over the same attribute, or straight usage and a tx -> we have
		// to preserve the
		// original geometry as well, thus we need cloning
		final StyleAttributeExtractor extractor = new StyleAttributeExtractor();
		final FeatureType featureType = layer.getFeatureSource().getSchema();
		final Set<String> plainGeometries = new java.util.HashSet<String>();
		final Set<String> txGeometries = new java.util.HashSet<String>();
		for (final LiteFeatureTypeStyle lft : lfts) {
			for (final Rule r : lft.ruleList) {
				for (final Symbolizer s : r.symbolizers()) {
					if (s.getGeometry() == null) {
						final String attribute = featureType.getGeometryDescriptor().getName().getLocalPart();
						if (txGeometries.contains(attribute)) {
							return true;
						}
						plainGeometries.add(attribute);
					}
					else if (s.getGeometry() instanceof PropertyName) {
						final String attribute = ((PropertyName) s.getGeometry()).getPropertyName();
						if (txGeometries.contains(attribute)) {
							return true;
						}
						plainGeometries.add(attribute);
					}
					else {
						final Expression g = s.getGeometry();
						extractor.clear();
						g.accept(
								extractor,
								null);
						final Set<String> attributes = extractor.getAttributeNameSet();
						for (final String attribute : attributes) {
							if (plainGeometries.contains(attribute)) {
								return true;
							}
							if (txGeometries.contains(attribute)) {
								return true;
							}
							txGeometries.add(attribute);
						}
					}
				}
			}
		}

		return false;
	}

	/**
	 * Returns the generalization distance in the screen space.
	 * 
	 */
	@Override
	public double getGeneralizationDistance() {
		return generalizationDistance;
	}

	/**
	 * <p>
	 * Sets the generalizazion distance in the screen space.
	 * </p>
	 * <p>
	 * Default value is 0.8, meaning that two subsequent points are collapsed to
	 * one if their on screen distance is less than one pixel
	 * </p>
	 * <p>
	 * Set the distance to 0 if you don't want any kind of generalization
	 * </p>
	 * 
	 * @param d
	 */
	@Override
	public void setGeneralizationDistance(
			final double d ) {
		generalizationDistance = d;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.geotools.renderer.GTRenderer#setJava2DHints(java.awt.RenderingHints)
	 */
	@Override
	public void setJava2DHints(
			final RenderingHints hints ) {
		java2dHints = hints;
		styleFactory.setRenderingHints(hints);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.geotools.renderer.GTRenderer#getJava2DHints()
	 */
	@Override
	public RenderingHints getJava2DHints() {
		return java2dHints;
	}

	@Override
	public void setRendererHints(
			final Map hints ) {
		if ((hints != null) && hints.containsKey(LABEL_CACHE_KEY)) {
			final LabelCache cache = (LabelCache) hints.get(LABEL_CACHE_KEY);
			if (cache == null) {
				throw new NullPointerException(
						"Label_Cache_Hint has a null value for the labelcache");
			}

			labelCache = cache;
		}
		if ((hints != null) && hints.containsKey(LINE_WIDTH_OPTIMIZATION_KEY)) {
			styleFactory.setLineOptimizationEnabled(Boolean.TRUE.equals(hints.get(LINE_WIDTH_OPTIMIZATION_KEY)));
		}
		rendererHints = hints;

		super.setRendererHints(hints);

		// sets whether vector rendering is enabled in the SLDStyleFactory
		styleFactory.setVectorRenderingEnabled(isVectorRenderingEnabled());
	}

	protected boolean isLineWidthOptimizationEnabled() {
		if ((rendererHints != null) && rendererHints.containsKey(LINE_WIDTH_OPTIMIZATION_KEY)) {
			return Boolean.TRUE.equals(rendererHints.get(LINE_WIDTH_OPTIMIZATION_KEY));
		}
		return DefaultWebMapService.isLineWidthOptimizationEnabled();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.geotools.renderer.GTRenderer#getRendererHints()
	 */
	@Override
	public Map getRendererHints() {
		return rendererHints;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @deprecated The {@code MapContext} class is being phased out. Please use
	 *             {@link #setMapContent}.
	 */
	@Override
	@Deprecated
	public void setContext(
			final MapContext context ) {
		// MapContext isA MapContent
		mapContent = context;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @deprecated The {@code MapContext} class is being phased out. Please use
	 *             {@link #setMapContent}.
	 */
	@Override
	@Deprecated
	public MapContext getContext() {
		if (mapContent instanceof MapContext) {
			return (MapContext) mapContent;
		}
		else {
			final MapContext context = new MapContext(
					mapContent);
			return context;
		}
	}

	@Override
	public void setMapContent(
			final MapContent mapContent ) {
		this.mapContent = mapContent;
	}

	@Override
	public MapContent getMapContent() {
		return mapContent;
	}

	@Override
	public boolean isCanTransform() {
		return canTransform;
	}

	public static MathTransform getMathTransform(
			final CoordinateReferenceSystem sourceCRS,
			final CoordinateReferenceSystem destCRS ) {
		try {
			return CRS.findMathTransform(
					sourceCRS,
					destCRS,
					true);
		}
		catch (final OperationNotFoundException e) {
			LOGGER.log(
					Level.SEVERE,
					e.getLocalizedMessage(),
					e);

		}
		catch (final FactoryException e) {
			LOGGER.log(
					Level.SEVERE,
					e.getLocalizedMessage(),
					e);
		}
		return null;
	}

	/**
	 * Builds a raster grid geometry that will be used for reading, taking into
	 * account the original map extent and target paint area, and expanding the
	 * target raster area by {@link #REPROJECTION_RASTER_GUTTER}
	 * 
	 * @param destinationCrs
	 * @param sourceCRS
	 * @return
	 * @throws NoninvertibleTransformException
	 */
	GridGeometry2D getRasterGridGeometry(
			final CoordinateReferenceSystem destinationCrs,
			final CoordinateReferenceSystem sourceCRS )
			throws NoninvertibleTransformException {
		GridGeometry2D readGG;
		if ((sourceCRS == null) || (destinationCrs == null) || CRS.equalsIgnoreMetadata(
				destinationCrs,
				sourceCRS)) {
			readGG = new GridGeometry2D(
					new GridEnvelope2D(
							screenSize),
					originalMapExtent);
		}
		else {
			// reprojection involved, read a bit more pixels to account for
			// rotation
			final Rectangle bufferedTargetArea = (Rectangle) screenSize.clone();
			bufferedTargetArea.add(
					// exand top/right
					screenSize.x + screenSize.width + REPROJECTION_RASTER_GUTTER,
					screenSize.y + screenSize.height + REPROJECTION_RASTER_GUTTER);
			bufferedTargetArea.add(
					// exand bottom/left
					screenSize.x - REPROJECTION_RASTER_GUTTER,
					screenSize.y - REPROJECTION_RASTER_GUTTER);

			// now create the final envelope accordingly
			readGG = new GridGeometry2D(
					new GridEnvelope2D(
							bufferedTargetArea),
					PixelInCell.CELL_CORNER,
					new AffineTransform2D(
							worldToScreenTransform.createInverse()),
					originalMapExtent.getCoordinateReferenceSystem(),
					null);
		}
		return readGG;
	}

	/**
	 * <p>
	 * Returns the rendering buffer, a measure in pixels used to expand the
	 * geometry search area enough to capture the geometries that do stay
	 * outside of the current rendering bounds but do affect them because of
	 * their large strokes (labels and graphic symbols are handled differently,
	 * see the label chache).
	 * </p>
	 * 
	 */
	private int getRenderingBuffer() {
		if (rendererHints == null) {
			return renderingBufferDEFAULT;
		}
		final Number result = (Number) rendererHints.get("renderingBuffer");
		if (result == null) {
			return renderingBufferDEFAULT;
		}
		return result.intValue();
	}

	/**
	 * <p>
	 * Returns scale computation algorithm to be used.
	 * </p>
	 * 
	 */
	private String getScaleComputationMethod() {
		if (rendererHints == null) {
			return scaleComputationMethodDEFAULT;
		}
		final String result = (String) rendererHints.get("scaleComputationMethod");
		if (result == null) {
			return scaleComputationMethodDEFAULT;
		}
		return result;
	}

	/**
	 * Returns the text rendering method
	 */
	private String getTextRenderingMethod() {
		if (rendererHints == null) {
			return textRenderingModeDEFAULT;
		}
		final String result = (String) rendererHints.get(TEXT_RENDERING_KEY);
		if (result == null) {
			return textRenderingModeDEFAULT;
		}
		return result;
	}

	GridCoverage2D readCoverage(
			final GridCoverage2DReader reader,
			final Object params,
			final GridGeometry2D readGG )
			throws IOException {
		GridCoverage2D coverage;
		// read the coverage with the proper target geometry (will trigger
		// cropping and resolution reduction)
		final Parameter<GridGeometry2D> readGGParam = new Parameter<GridGeometry2D>(
				AbstractGridFormat.READ_GRIDGEOMETRY2D);
		readGGParam.setValue(readGG);
		// then I try to get read parameters associated with this
		// coverage if there are any.
		if (params != null) {
			// //
			//
			// Getting parameters to control how to read this coverage.
			// Remember to check to actually have them before forwarding
			// them to the reader.
			//
			// //
			final GeneralParameterValue[] readParams = (GeneralParameterValue[]) params;
			final int length = readParams.length;
			if (length > 0) {
				// we have a valid number of parameters, let's check if
				// also have a READ_GRIDGEOMETRY2D. In such case we just
				// override it with the one we just build for this
				// request.
				final String name = AbstractGridFormat.READ_GRIDGEOMETRY2D.getName().toString();
				int i = 0;
				for (; i < length; i++) {
					if (readParams[i].getDescriptor().getName().toString().equalsIgnoreCase(
							name)) {
						break;
					}
				}
				// did we find anything?
				if (i < length) {
					// we found another READ_GRIDGEOMETRY2D, let's override it.
					((Parameter) readParams[i]).setValue(readGGParam);
					coverage = reader.read(readParams);
				}
				else {
					// add the correct read geometry to the supplied
					// params since we did not find anything
					final GeneralParameterValue[] readParams2 = new GeneralParameterValue[length + 1];
					System.arraycopy(
							readParams,
							0,
							readParams2,
							0,
							length);
					readParams2[length] = readGGParam;
					coverage = reader.read(readParams2);
				}
			}
			else {
				// we have no parameters hence we just use the read grid
				// geometry to get a coverage
				coverage = reader.read(new GeneralParameterValue[] {
					readGGParam
				});
			}
		}
		else if (readGG != null) {
			coverage = reader.read(new GeneralParameterValue[] {
				readGGParam
			});
		}
		else {
			coverage = reader.read(null);
		}
		return coverage;
	}

	/**
	 * A request sent to the painting thread
	 * 
	 */
	abstract class RenderingRequest
	{
		abstract void execute();
	}

	/**
	 * A request to paint a shape with a specific Style2D
	 * 
	 * 
	 */
	class PaintShapeRequest extends
			RenderingRequest
	{
		Graphics2D graphic;

		LiteShape2 shape;

		Style2D style;

		double scale;

		boolean labelObstacle = false;

		public PaintShapeRequest(
				final Graphics2D graphic,
				final LiteShape2 shape,
				final Style2D style,
				final double scale ) {
			this.graphic = graphic;
			this.shape = shape;
			this.style = style;
			this.scale = scale;
		}

		public void setLabelObstacle(
				final boolean labelObstacle ) {
			this.labelObstacle = labelObstacle;
		}

		@Override
		void execute() {
			if (graphic instanceof DelayedBackbufferGraphic) {
				((DelayedBackbufferGraphic) graphic).init();
			}

			try {
				painter.paint(
						graphic,
						shape,
						style,
						scale,
						labelObstacle);
			}
			catch (final Throwable t) {
				fireErrorEvent(t);
			}
		}
	}

	/**
	 * A request to paint a shape with a specific Style2D
	 * 
	 * 
	 */
	class FeatureRenderedRequest extends
			RenderingRequest
	{
		Object content;

		public FeatureRenderedRequest(
				final Object content ) {
			this.content = content;
		}

		@Override
		void execute() {
			fireFeatureRenderedEvent(content);
		}
	}

	/**
	 * A request to merge multiple back buffers to the main graphics
	 * 
	 * 
	 */
	class MergeLayersRequest extends
			RenderingRequest
	{
		Graphics2D graphics;
		BufferedImage mergeGraphics[];

		public MergeLayersRequest(
				final Graphics2D graphics,
				final BufferedImage[] mergeGraphics ) {
			this.graphics = graphics;
			this.mergeGraphics = mergeGraphics;
		}

		@Override
		void execute() {
			graphics.setComposite(AlphaComposite.getInstance(AlphaComposite.SRC_OVER));
			for (final BufferedImage ftsGraphics : mergeGraphics) {
				// we may have not found anything to paint, in that case the
				// delegate
				// has not been initialized
				if (ftsGraphics != null) {
					graphics.drawImage(
							ftsGraphics,
							0,
							0,
							null);
				}
			}

		}
	}

	/**
	 * A request to render a raster
	 *  
	 */
	public class RenderRasterRequest extends
			RenderingRequest
	{

		private final Graphics2D graphics;
		private final boolean disposeCoverage;
		private final GridCoverage2D coverage;
		private final RasterSymbolizer symbolizer;
		private final CoordinateReferenceSystem destinationCRS;
		private final AffineTransform worldToScreen;

		public RenderRasterRequest(
				final Graphics2D graphics,
				final GridCoverage2D coverage,
				final boolean disposeCoverage,
				final RasterSymbolizer symbolizer,
				final CoordinateReferenceSystem destinationCRS,
				final AffineTransform worldToScreen ) {
			this.graphics = graphics;
			this.coverage = coverage;
			this.disposeCoverage = disposeCoverage;
			this.symbolizer = symbolizer;
			this.destinationCRS = destinationCRS;
			this.worldToScreen = worldToScreen;
		}

		@Override
		void execute() {
			if (LOGGER.isLoggable(Level.FINE)) {
				LOGGER.fine("Rendering Raster " + coverage);
			}

			try {
				// /////////////////////////////////////////////////////////////////
				//
				// If the grid object is a reader we ask him to do its best for
				// the
				// requested resolution, if it is a gridcoverage instead we have
				// to
				// rely on the gridocerage renderer itself.
				//
				// /////////////////////////////////////////////////////////////////
				final GridCoverageRenderer gcr = new GridCoverageRenderer(
						destinationCRS,
						originalMapExtent,
						screenSize,
						worldToScreen,
						java2dHints);

				try {
					gcr.paint(
							graphics,
							coverage,
							symbolizer);
				}
				finally {
					// we need to try and dispose this coverage if was created
					// on purpose for
					// rendering
					if ((coverage != null) && disposeCoverage) {
						coverage.dispose(true);
						final RenderedImage image = coverage.getRenderedImage();
						if (image instanceof PlanarImage) {
							ImageUtilities.disposePlanarImageChain((PlanarImage) image);
						}
					}
				}

				if (LOGGER.isLoggable(Level.FINE)) {
					LOGGER.fine("Raster rendered");
				}

			}
			catch (final FactoryException e) {
				LOGGER.log(
						Level.WARNING,
						e.getLocalizedMessage(),
						e);
				fireErrorEvent(e);
			}
			catch (final TransformException e) {
				LOGGER.log(
						Level.WARNING,
						e.getLocalizedMessage(),
						e);
				fireErrorEvent(e);
			}
			catch (final NoninvertibleTransformException e) {
				LOGGER.log(
						Level.WARNING,
						e.getLocalizedMessage(),
						e);
				fireErrorEvent(e);
			}
			catch (final IllegalArgumentException e) {
				LOGGER.log(
						Level.WARNING,
						e.getLocalizedMessage(),
						e);
				fireErrorEvent(e);
			}
		}
	}

	class RenderDirectLayerRequest extends
			RenderingRequest
	{
		private final Graphics2D graphics;
		private final DirectLayer layer;

		public RenderDirectLayerRequest(
				final Graphics2D graphics,
				final DirectLayer layer ) {
			this.graphics = graphics;
			this.layer = layer;
		}

		@Override
		void execute() {
			if (LOGGER.isLoggable(Level.FINE)) {
				LOGGER.fine("Rendering DirectLayer: " + layer);
			}

			try {
				layer.draw(
						graphics,
						mapContent,
						mapContent.getViewport());

				if (LOGGER.isLoggable(Level.FINE)) {
					LOGGER.fine("Layer rendered");
				}
			}
			catch (final Exception e) {
				LOGGER.log(
						Level.WARNING,
						e.getLocalizedMessage(),
						e);
				fireErrorEvent(e);
			}
		}

	}

	/**
	 * Marks the end of the request flow, instructs the painting thread to exit
	 * 
	 */
	class EndRequest extends
			RenderingRequest
	{

		@Override
		void execute() {
			// nothing to do here
		}

	}

	/**
	 * The secondary thread that actually issues the paint requests against the
	 * graphic object
	 *  
	 */
	class PainterThread implements
			Runnable
	{
		BlockingQueue<RenderingRequest> requests;
		Thread thread;

		public PainterThread(
				final BlockingQueue<RenderingRequest> requests ) {
			this.requests = requests;
		}

		public void interrupt() {
			if (thread != null) {
				thread.interrupt();
			}
		}

		@Override
		public void run() {
			thread = Thread.currentThread();
			boolean done = false;
			while (!done) {
				try {
					final RenderingRequest request = requests.take();
					if ((request instanceof EndRequest) || renderingStopRequested) {
						done = true;
					}
					else {
						request.execute();
					}
				}
				catch (final InterruptedException e) {
					// ok, we might have been interupped to stop processing
					if (renderingStopRequested) {
						done = true;
					}
				}
				catch (final Throwable t) {
					fireErrorEvent(t);
				}

			}

		}

	}

	/**
	 * A blocking queue subclass with a special behavior for the occasion when
	 * the rendering stop has been requested: puts are getting ignored, and take
	 * always returns an EndRequest
	 * 
	 * 
	 */
	class RenderingBlockingQueue extends
			ArrayBlockingQueue<RenderingRequest>
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public RenderingBlockingQueue(
				final int capacity ) {
			super(
					capacity);
		}

		@Override
		public void put(
				final RenderingRequest e )
				throws InterruptedException {
			if (!renderingStopRequested) {
				super.put(e);
				if (renderingStopRequested) {
					clear();
				}
			}
		}

		@Override
		public RenderingRequest take()
				throws InterruptedException {
			if (!renderingStopRequested) {
				return super.take();
			}
			else {
				return new EndRequest();
			}
		}

	}

}
