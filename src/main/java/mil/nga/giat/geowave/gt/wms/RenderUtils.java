package mil.nga.giat.geowave.gt.wms;

import java.awt.geom.AffineTransform;

import org.geotools.factory.Hints;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.operation.transform.ConcatenatedTransform;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.MathTransform2D;

public class RenderUtils
{
	/**
	 * Builds a full transform going from the source CRS to the destination CRS
	 * and from there to the screen.
	 * <p>
	 * Although we ask for 2D content (via {@link Hints#FEATURE_2D} ) not all
	 * DataStore implementations are capable. In this event we will manually
	 * stage the information into {@link DefaultGeographicCRS#WGS84}) and before
	 * using this transform.
	 */
	// this is a snippet generally extracted from
	// org.geotools.renderer.lite.StreamingRenderer
	// lines 1213-1227
	public static MathTransform buildFullTransform(
			final CoordinateReferenceSystem sourceCRS,
			final CoordinateReferenceSystem destCRS,
			final AffineTransform worldToScreenTransform )
			throws FactoryException {
		MathTransform mt = buildTransform(
				sourceCRS,
				destCRS);

		// concatenate from world to screen
		if ((mt != null) && !mt.isIdentity()) {
			mt = ConcatenatedTransform.create(
					mt,
					ProjectiveTransform.create(worldToScreenTransform));
		}
		else {
			mt = ProjectiveTransform.create(worldToScreenTransform);
		}

		return mt;
	}

	/**
	 * Builds the transform from sourceCRS to destCRS/
	 * <p>
	 * Although we ask for 2D content (via {@link Hints#FEATURE_2D} ) not all
	 * DataStore implementations are capable. With that in mind if the provided
	 * soruceCRS is not 2D we are going to manually post-process the Geomtries
	 * into {@link DefaultGeographicCRS#WGS84} - and the {@link MathTransform2D}
	 * returned here will transition from WGS84 to the requested destCRS.
	 * 
	 * @param sourceCRS
	 * @param destCRS
	 * @return the transform, or null if any of the crs is null, or if the the
	 *         two crs are equal
	 * @throws FactoryException
	 *             If no transform is available to the destCRS
	 */
	// this is a snippet generally extracted from
	// org.geotools.renderer.lite.StreamingRenderer
	// lines 1242-1270
	public static MathTransform buildTransform(
			CoordinateReferenceSystem sourceCRS,
			final CoordinateReferenceSystem destCRS )
			throws FactoryException {
		MathTransform transform = null;
		if ((sourceCRS != null) && (sourceCRS.getCoordinateSystem().getDimension() >= 3)) {
			// We are going to transform over to DefaultGeographic.WGS84 on the
			// fly
			// so we will set up our math transform to take it from there
			final MathTransform toWgs84_3d = CRS.findMathTransform(
					sourceCRS,
					DefaultGeographicCRS.WGS84_3D);
			final MathTransform toWgs84_2d = CRS.findMathTransform(
					DefaultGeographicCRS.WGS84_3D,
					DefaultGeographicCRS.WGS84);
			transform = ConcatenatedTransform.create(
					toWgs84_3d,
					toWgs84_2d);
			sourceCRS = DefaultGeographicCRS.WGS84;
		}
		// the basic crs transformation, if any
		MathTransform2D mt;
		if ((sourceCRS == null) || (destCRS == null) || CRS.equalsIgnoreMetadata(
				sourceCRS,
				destCRS)) {
			mt = null;
		}
		else {
			mt = (MathTransform2D) CRS.findMathTransform(
					sourceCRS,
					destCRS,
					true);
		}

		if (transform != null) {
			if (mt == null) {
				return transform;
			}
			else {
				return ConcatenatedTransform.create(
						transform,
						mt);
			}
		}
		else {
			return mt;
		}
	}
}
