package mil.nga.giat.geowave.gt.wms;

import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import mil.nga.giat.geowave.gt.wms.accumulo.RenderedMaster;
import mil.nga.giat.geowave.gt.wms.accumulo.RenderedStyle;

public class DistributedRenderResultStore
{
	private final static class StyleImage
	{
		private final Map<String, BufferedImage> images = new TreeMap<String, BufferedImage>();

		private void addRenderedStyle(
				final String resultId,
				final RenderedStyle style ) {
			images.put(
					resultId,
					style.getImage());
		}
	}

	private final Map<String, BufferedImage> masterImagesToMerge = new TreeMap<String, BufferedImage>();
	private final Map<String, StyleImage> styleImagesToMerge = new HashMap<String, StyleImage>();

	public void addResult(
			final String resultId,
			final RenderedMaster result ) {
		if (result.getImage() != null) {
			masterImagesToMerge.put(
					resultId,
					result.getImage());
		}
		for (final RenderedStyle s : result.getRenderedStyles()) {
			StyleImage img = styleImagesToMerge.get(s.getStyleId());
			if (img == null) {
				img = new StyleImage();
				styleImagesToMerge.put(
						s.getStyleId(),
						img);
			}
			img.addRenderedStyle(
					resultId,
					s);
		}
	}

	public BufferedImage[] getDrawOrderResults(
			final String[] styleIdDrawOrder ) {
		final List<BufferedImage> orderedImages = new ArrayList<>();
		// first do the styles in order
		for (final String styleId : styleIdDrawOrder) {
			final StyleImage styleImage = styleImagesToMerge.get(styleId);
			if (styleImage != null) {
				// add the images in order of the result key
				orderedImages.addAll(styleImage.images.values());
			}
		}
		// then the master (which are typically just labels), in order of result
		// key
		orderedImages.addAll(masterImagesToMerge.values());
		return orderedImages.toArray(new BufferedImage[] {});
	}
}
