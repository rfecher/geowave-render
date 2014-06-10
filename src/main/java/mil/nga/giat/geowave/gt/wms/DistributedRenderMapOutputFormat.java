package mil.nga.giat.geowave.gt.wms;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.image.IndexColorModel;
import java.awt.image.RenderedImage;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.media.jai.ImageLayout;
import javax.media.jai.Interpolation;
import javax.media.jai.InterpolationBicubic2;
import javax.media.jai.InterpolationBilinear;
import javax.media.jai.InterpolationNearest;
import javax.media.jai.JAI;
import javax.media.jai.LookupTableJAI;
import javax.media.jai.operator.LookupDescriptor;

import mil.nga.giat.geowave.gt.wms.accumulo.ServerDecimationOptions;

import org.geoserver.platform.ServiceException;
import org.geoserver.wms.DefaultWebMapService;
import org.geoserver.wms.GetMapRequest;
import org.geoserver.wms.WMS;
import org.geoserver.wms.WMSInfo.WMSInterpolation;
import org.geoserver.wms.WMSMapContent;
import org.geoserver.wms.decoration.MapDecorationLayout;
import org.geoserver.wms.map.ImageUtils;
import org.geoserver.wms.map.KMLStyleFilteringVisitor;
import org.geoserver.wms.map.MaxErrorEnforcer;
import org.geoserver.wms.map.MetatileMapOutputFormat;
import org.geoserver.wms.map.PaletteExtractor;
import org.geoserver.wms.map.RenderExceptionStrategy;
import org.geoserver.wms.map.RenderedImageMap;
import org.geoserver.wms.map.RenderedImageMapOutputFormat;
import org.geoserver.wms.map.RenderingTimeoutEnforcer;
import org.geotools.map.Layer;
import org.geotools.map.StyleLayer;
import org.geotools.renderer.lite.StreamingRenderer;
import org.geotools.styling.Style;
import org.geotools.util.logging.Logging;

public class DistributedRenderMapOutputFormat extends
		RenderedImageMapOutputFormat
{
	protected static final String DISTRIBUTED_RENDERING_KEY = "distributed-rendering";
	protected static final boolean DEFAULT_DISTRIBUTED_RENDERING = false;
	protected static final String DISTRIBUTED_DECIMATED_RENDERING_KEY = "decimated-rendering";
	protected static final boolean DEFAULT_DISTRIBUTED_DECIMATED_RENDERING = false;
	protected static final String DECIMATION_ALPHA_THRESHOLD_KEY = "alpha-threshold";
	protected static final Double DEFAULT_DECIMATION_ALPHA_THRESHOLD = null;
	protected static final String DECIMATION_COUNT_THRESHOLD_KEY = "count-threshold";
	protected static final Integer DEFAULT_DECIMATION_COUNT_THRESHOLD = null;
	protected static final String DECIMATION_PIXEL_SIZE_KEY = "pixel-size";
	protected static final Integer DEFAULT_DECIMATION_PIXEL_SIZE = null;
	protected static final String USE_SECONDARY_STYLES_KEY = "use-secondary-styles";
	protected static final boolean DEFAULT_USE_SECONDARY_STYLES = true;

	/** A logger for this class. */
	private static final Logger LOGGER = Logging.getLogger(DistributedRenderMapOutputFormat.class);

	private final static Interpolation NN_INTERPOLATION = new InterpolationNearest();

	private final static Interpolation BIL_INTERPOLATION = new InterpolationBilinear();

	private final static Interpolation BIC_INTERPOLATION = new InterpolationBicubic2(
			0);

	// antialiasing settings, no antialias, only text, full antialias
	private final static String AA_NONE = "NONE";

	private final static String AA_TEXT = "TEXT";

	private final static String AA_FULL = "FULL";

	private final static List<String> AA_SETTINGS = Arrays.asList(new String[] {
		AA_NONE,
		AA_TEXT,
		AA_FULL
	});

	/**
	 * The size of a megabyte
	 */
	private static final int KB = 1024;

	/**
	 * The lookup table used for data type transformation (it's really the
	 * identity one)
	 */
	private static LookupTableJAI IDENTITY_TABLE = new LookupTableJAI(
			getTable());

	private static byte[] getTable() {
		final byte[] arr = new byte[256];
		for (int i = 0; i < arr.length; i++) {
			arr[i] = (byte) i;
		}
		return arr;
	}

	public DistributedRenderMapOutputFormat(
			final String mime,
			final String[] outputFormats,
			final WMS wms ) {
		super(
				mime,
				outputFormats,
				wms);
	}

	public DistributedRenderMapOutputFormat(
			final String mime,
			final WMS wms ) {
		super(
				mime,
				wms);
	}

	public DistributedRenderMapOutputFormat(
			final WMS wms ) {
		super(
				wms);
	}

	protected static boolean enableDistributedRender(
			final WMSMapContent mapContent ) {
		boolean retVal = false;
		for (final Layer l : mapContent.layers()) {
			if (evaluateKeywords(l)) {
				// don't just return true immediately, as we want to cache a
				// boolean result for enabling/disabling the distributed
				// rendering per layer
				retVal = true;
			}
		}
		return retVal;
	}

	protected static boolean evaluateKeywords(
			final Layer layer ) {
		boolean retVal = false;
		final Properties properties = getProperties(layer);
		if (properties == null) {
			return false;
		}
		if (isDistributedRenderingEnabled(properties)) {
			layer.getUserData().put(
					DISTRIBUTED_RENDERING_KEY,
					true);
			retVal = true;
		}
		else {
			layer.getUserData().put(
					DISTRIBUTED_RENDERING_KEY,
					false);
		}
		if (isDistributedDecimatedRenderingEnabled(properties)) {
			// if it is enabled, continue to parse decimation options
			layer.getUserData().put(
					DISTRIBUTED_DECIMATED_RENDERING_KEY,
					getDecimationOptions(properties));
			retVal = true;
		}
		return retVal;
	}

	protected static Properties getProperties(
			final Layer layer ) {
		final Object obj = layer.getUserData().get(
				"abstract");
		if (obj != null) {
			final Properties properties = new Properties();
			try {
				properties.load(new StringReader(
						obj.toString()));
			}
			catch (final Exception e) {
				// merely log as info, considering this layer may not have been
				// intended to be used this way
				LOGGER.log(
						Level.INFO,
						"unable to read layer abstract as properties",
						e);
			}
			return properties;
		}
		return null;
	}

	protected static boolean isDistributedRenderingEnabled(
			final Properties properties ) {
		return getBoolean(
				properties,
				DISTRIBUTED_RENDERING_KEY,
				DEFAULT_DISTRIBUTED_RENDERING);
	}

	protected static boolean isDistributedDecimatedRenderingEnabled(
			final Properties properties ) {
		return getBoolean(
				properties,
				DISTRIBUTED_DECIMATED_RENDERING_KEY,
				DEFAULT_DISTRIBUTED_DECIMATED_RENDERING);
	}

	protected static boolean getBoolean(
			final Properties properties,
			final String key,
			final boolean defaultValue ) {
		if (!properties.containsKey(key)) {
			return defaultValue;
		}
		final Object obj = properties.getProperty(key);
		try {
			if (obj != null) {
				return Boolean.parseBoolean(obj.toString().trim());
			}
		}
		catch (final Exception e) {
			LOGGER.log(
					Level.WARNING,
					"unable to parse as boolean " + key,
					e);
		}
		return false;
	}

	protected static Integer getInteger(
			final Properties properties,
			final String key,
			final Integer defaultValue ) {
		if (!properties.containsKey(key)) {
			return defaultValue;
		}
		final Object obj = properties.getProperty(key);
		try {
			if (obj != null) {
				return Integer.parseInt(obj.toString().trim());
			}
		}
		catch (final Exception e) {
			LOGGER.log(
					Level.WARNING,
					"unable to parse as integer " + key,
					e);
		}
		return null;
	}

	protected static Double getDouble(
			final Properties properties,
			final String key,
			final Double defaultValue ) {
		if (!properties.containsKey(key)) {
			return defaultValue;
		}
		final Object obj = properties.getProperty(key);
		try {
			if (obj != null) {
				return Double.parseDouble(obj.toString().trim());
			}
		}
		catch (final Exception e) {
			LOGGER.log(
					Level.WARNING,
					"unable to parse as double " + key,
					e);
		}
		return null;
	}

	protected static ServerDecimationOptions getDecimationOptions(
			final Properties properties ) {
		return new ServerDecimationOptions(
				getBoolean(
						properties,
						USE_SECONDARY_STYLES_KEY,
						DEFAULT_USE_SECONDARY_STYLES),
				getDouble(
						properties,
						DECIMATION_ALPHA_THRESHOLD_KEY,
						DEFAULT_DECIMATION_ALPHA_THRESHOLD),
				getInteger(
						properties,
						DECIMATION_COUNT_THRESHOLD_KEY,
						DEFAULT_DECIMATION_COUNT_THRESHOLD),
				getInteger(
						properties,
						DECIMATION_PIXEL_SIZE_KEY,
						DEFAULT_DECIMATION_PIXEL_SIZE));
	}

	@Override
	public RenderedImageMap produceMap(
			final WMSMapContent mapContent,
			final boolean tiled )
			throws ServiceException {
		if (!enableDistributedRender(mapContent)) {
			return super.produceMap(
					mapContent,
					tiled);
		}
		final Rectangle paintArea = new Rectangle(
				0,
				0,
				mapContent.getMapWidth(),
				mapContent.getMapHeight());

		if (LOGGER.isLoggable(Level.FINE)) {
			LOGGER.fine("setting up " + paintArea.width + "x" + paintArea.height + " image");
		}

		// extra antialias setting
		final GetMapRequest request = mapContent.getRequest();
		String antialias = (String) request.getFormatOptions().get(
				"antialias");
		if (antialias != null) {
			antialias = antialias.toUpperCase();
		}

		// figure out a palette for buffered image creation
		IndexColorModel palette = null;
		final boolean transparent = mapContent.isTransparent() && isTransparencySupported();
		final Color bgColor = mapContent.getBgColor();
		if (AA_NONE.equals(antialias)) {
			palette = mapContent.getPalette();
		}
		else if (AA_NONE.equals(antialias)) {
			final PaletteExtractor pe = new PaletteExtractor(
					transparent ? null : bgColor);
			final List<Layer> layers = mapContent.layers();
			for (int i = 0; i < layers.size(); i++) {
				pe.visit(layers.get(
						i).getStyle());
				if (!pe.canComputePalette()) {
					break;
				}
			}
			if (pe.canComputePalette()) {
				palette = pe.getPalette();
			}
		}

		// before even preparing the rendering surface, check it's not too big,
		// if so, throw a service exception
		final long maxMemory = wms.getMaxRequestMemory() * KB;
		// ... base image memory
		long memory = getDrawingSurfaceMemoryUse(
				paintArea.width,
				paintArea.height,
				palette,
				transparent);
		// .. use a fake streaming renderer to evaluate the extra back buffers
		// used when rendering
		// multiple featureTypeStyles against the same layer
		final StreamingRenderer testRenderer = new StreamingRenderer();
		testRenderer.setMapContent(mapContent);
		memory += testRenderer.getMaxBackBufferMemory(
				paintArea.width,
				paintArea.height);
		if ((maxMemory > 0) && (memory > maxMemory)) {
			final long kbUsed = memory / KB;
			final long kbMax = maxMemory / KB;
			throw new ServiceException(
					"Rendering request would use " + kbUsed + "KB, whilst the " + "maximum memory allowed is " + kbMax + "KB");
		}

		final MapDecorationLayout layout = findDecorationLayout(
				request,
				tiled);

		// TODO: allow rendering to continue with vector layers
		// TODO: allow rendering to continue with layout
		// TODO: handle rotated rasters
		// TODO: handle color conversions
		// TODO: handle meta-tiling
		// TODO: how to handle timeout here? I guess we need to move it into the
		// dispatcher?

		RenderedImage image = null;
		// fast path for pure coverage rendering
		if (DefaultWebMapService.isDirectRasterPathEnabled() && (mapContent.layers().size() == 1) && (mapContent.getAngle() == 0.0) && ((layout == null) || layout.isEmpty())) {
			/** this is modified so as not to copy the direct raster rendering **/
			// return super.produceMap(mapContent, tiled);
		}

		// we use the alpha channel if the image is transparent or if the meta
		// tiler
		// is enabled, since apparently the Crop operation inside the meta-tiler
		// generates striped images in that case (see GEOS-
		final boolean useAlpha = transparent || MetatileMapOutputFormat.isRequestTiled(
				request,
				this);
		final RenderedImage preparedImage = prepareImage(
				paintArea.width,
				paintArea.height,
				palette,
				useAlpha);
		final Map<RenderingHints.Key, Object> hintsMap = new HashMap<RenderingHints.Key, Object>();

		final Graphics2D graphic = ImageUtils.prepareTransparency(
				transparent,
				bgColor,
				preparedImage,
				hintsMap);

		// set up the antialias hints
		if (AA_NONE.equals(antialias)) {
			hintsMap.put(
					RenderingHints.KEY_ANTIALIASING,
					RenderingHints.VALUE_ANTIALIAS_OFF);
			if (preparedImage.getColorModel() instanceof IndexColorModel) {
				// otherwise we end up with dithered colors where the match is
				// not 100%
				hintsMap.put(
						RenderingHints.KEY_DITHERING,
						RenderingHints.VALUE_DITHER_DISABLE);
			}
		}
		else if (AA_TEXT.equals(antialias)) {
			hintsMap.put(
					RenderingHints.KEY_ANTIALIASING,
					RenderingHints.VALUE_ANTIALIAS_OFF);
			hintsMap.put(
					RenderingHints.KEY_TEXT_ANTIALIASING,
					RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
		}
		else {
			if ((antialias != null) && !AA_FULL.equals(antialias)) {
				LOGGER.warning("Unrecognized antialias setting '" + antialias + "', valid values are " + AA_SETTINGS);
			}
			hintsMap.put(
					RenderingHints.KEY_ANTIALIASING,
					RenderingHints.VALUE_ANTIALIAS_ON);
		}

		// these two hints improve text layout in diagonal labels and reduce
		// artifacts
		// in line rendering (without hampering performance)
		hintsMap.put(
				RenderingHints.KEY_FRACTIONALMETRICS,
				RenderingHints.VALUE_FRACTIONALMETRICS_ON);
		hintsMap.put(
				RenderingHints.KEY_STROKE_CONTROL,
				RenderingHints.VALUE_STROKE_PURE);

		// turn off/on interpolation rendering hint
		if (wms != null) {
			if (WMSInterpolation.Nearest.equals(wms.getInterpolation())) {
				hintsMap.put(
						JAI.KEY_INTERPOLATION,
						NN_INTERPOLATION);
				hintsMap.put(
						RenderingHints.KEY_INTERPOLATION,
						RenderingHints.VALUE_INTERPOLATION_NEAREST_NEIGHBOR);
			}
			else if (WMSInterpolation.Bilinear.equals(wms.getInterpolation())) {
				hintsMap.put(
						JAI.KEY_INTERPOLATION,
						BIL_INTERPOLATION);
				hintsMap.put(
						RenderingHints.KEY_INTERPOLATION,
						RenderingHints.VALUE_INTERPOLATION_BILINEAR);
			}
			else if (WMSInterpolation.Bicubic.equals(wms.getInterpolation())) {
				hintsMap.put(
						JAI.KEY_INTERPOLATION,
						BIC_INTERPOLATION);
				hintsMap.put(
						RenderingHints.KEY_INTERPOLATION,
						RenderingHints.VALUE_INTERPOLATION_BICUBIC);
			}
		}

		// make sure the hints are set before we start rendering the map
		graphic.setRenderingHints(hintsMap);

		final RenderingHints hints = new RenderingHints(
				hintsMap);
		/**
		 * the renderer is the key variable that is modified from
		 * streamingrenderer to distributedrenderer
		 */
		final DistributedRenderer renderer = new DistributedRenderer();
		renderer.setThreadPool(DefaultWebMapService.getRenderingPool());
		renderer.setMapContent(mapContent);
		renderer.setJava2DHints(hints);

		// setup the renderer hints
		final Map<Object, Object> rendererParams = new HashMap<Object, Object>();
		rendererParams.put(
				"optimizedDataLoadingEnabled",
				new Boolean(
						true));
		rendererParams.put(
				"renderingBuffer",
				new Integer(
						mapContent.getBuffer()));
		rendererParams.put(
				"maxFiltersToSendToDatastore",
				DefaultWebMapService.getMaxFilterRules());
		rendererParams.put(
				StreamingRenderer.SCALE_COMPUTATION_METHOD_KEY,
				StreamingRenderer.SCALE_OGC);
		if (AA_NONE.equals(antialias)) {
			rendererParams.put(
					StreamingRenderer.TEXT_RENDERING_KEY,
					StreamingRenderer.TEXT_RENDERING_STRING);
		}
		else {
			rendererParams.put(
					StreamingRenderer.TEXT_RENDERING_KEY,
					StreamingRenderer.TEXT_RENDERING_ADAPTIVE);
		}
		if (DefaultWebMapService.isLineWidthOptimizationEnabled()) {
			rendererParams.put(
					StreamingRenderer.LINE_WIDTH_OPTIMIZATION_KEY,
					true);
		}

		// turn on advanced projection handling
		rendererParams.put(
				StreamingRenderer.ADVANCED_PROJECTION_HANDLING_KEY,
				true);
		if (DefaultWebMapService.isContinuousMapWrappingEnabled()) {
			rendererParams.put(
					StreamingRenderer.CONTINUOUS_MAP_WRAPPING,
					true);
		}

		// see if the user specified a dpi
		if (request.getFormatOptions().get(
				"dpi") != null) {
			rendererParams.put(
					StreamingRenderer.DPI_KEY,
					(request.getFormatOptions().get("dpi")));
		}

		boolean kmplacemark = false;
		if (request.getFormatOptions().get(
				"kmplacemark") != null) {
			kmplacemark = ((Boolean) request.getFormatOptions().get(
					"kmplacemark")).booleanValue();
		}
		if (kmplacemark) {
			// create a StyleVisitor that copies a style, but removes the
			// PointSymbolizers and TextSymbolizers
			final KMLStyleFilteringVisitor dupVisitor = new KMLStyleFilteringVisitor();

			// Remove PointSymbolizers and TextSymbolizers from the
			// layers' Styles to prevent their rendering on the
			// raster image. Both are better served with the
			// placemarks.
			final List<Layer> layers = mapContent.layers();
			for (int i = 0; i < layers.size(); i++) {
				if (layers.get(i) instanceof StyleLayer) {
					final StyleLayer layer = (StyleLayer) layers.get(i);
					final Style style = layer.getStyle();
					style.accept(dupVisitor);
					final Style copy = (Style) dupVisitor.getCopy();
					layer.setStyle(copy);
				}
			}
		}
		renderer.setRendererHints(rendererParams);

		// if abort already requested bail out
		// if (this.abortRequested) {
		// graphic.dispose();
		// return null;
		// }

		// enforce no more than x rendering errors
		final int maxErrors = wms.getMaxRenderingErrors();
		final MaxErrorEnforcer errorChecker = new MaxErrorEnforcer(
				renderer,
				maxErrors);

		// Add a render listener that ignores well known rendering exceptions
		// and reports back non
		// ignorable ones
		final RenderExceptionStrategy nonIgnorableExceptionListener;
		nonIgnorableExceptionListener = new RenderExceptionStrategy(
				renderer);
		renderer.addRenderListener(nonIgnorableExceptionListener);

		onBeforeRender(renderer);

		// setup the timeout enforcer (the enforcer is neutral when the timeout
		// is 0)
		final int maxRenderingTime = wms.getMaxRenderingTime() * 1000;
		final RenderingTimeoutEnforcer timeout = new RenderingTimeoutEnforcer(
				maxRenderingTime,
				renderer,
				graphic);
		timeout.start();
		try {
			// finally render the image;
			renderer.paint(
					graphic,
					paintArea,
					mapContent.getRenderingArea(),
					mapContent.getRenderingTransform(),
					useAlpha,
					bgColor);

			// apply watermarking
			if (layout != null) {
				try {
					layout.paint(
							graphic,
							paintArea,
							mapContent);
				}
				catch (final Exception e) {
					throw new ServiceException(
							"Problem occurred while trying to watermark data",
							e);
				}
			}
		}
		finally {
			timeout.stop();
			graphic.dispose();
		}

		// check if the request did timeout
		if (timeout.isTimedOut()) {
			throw new ServiceException(
					"This requested used more time than allowed and has been forcefully stopped. " + "Max rendering time is " + (maxRenderingTime / 1000.0) + "s");
		}

		// check if a non ignorable error occurred
		if (nonIgnorableExceptionListener.exceptionOccurred()) {
			final Exception renderError = nonIgnorableExceptionListener.getException();
			throw new ServiceException(
					"Rendering process failed",
					renderError,
					"internalError");
		}

		// check if too many errors occurred
		if (errorChecker.exceedsMaxErrors()) {
			throw new ServiceException(
					"More than " + maxErrors + " rendering errors occurred, bailing out.",
					errorChecker.getLastException(),
					"internalError");
		}

		if ((palette != null) && (palette.getMapSize() < 256)) {
			image = optimizeSampleModel(preparedImage);
		}
		else {
			image = preparedImage;
		}

		final RenderedImageMap map = buildMap(
				mapContent,
				image);
		return map;
	}

	/**
	 * This takes an image with an indexed color model that uses less than 256
	 * colors and has a 8bit sample model, and transforms it to one that has the
	 * optimal sample model (for example, 1bit if the palette only has 2 colors)
	 * 
	 * @param source
	 * @return
	 */
	private static RenderedImage optimizeSampleModel(
			final RenderedImage source ) {
		final int w = source.getWidth();
		final int h = source.getHeight();
		final ImageLayout layout = new ImageLayout();
		layout.setColorModel(source.getColorModel());
		layout.setSampleModel(source.getColorModel().createCompatibleSampleModel(
				w,
				h));
		// if I don't force tiling off with this setting an exception is thrown
		// when writing the image out...
		layout.setTileWidth(w);
		layout.setTileHeight(h);
		final RenderingHints hints = new RenderingHints(
				JAI.KEY_IMAGE_LAYOUT,
				layout);
		// TODO SIMONE why not format?
		return LookupDescriptor.create(
				source,
				IDENTITY_TABLE,
				hints);

	}
}
