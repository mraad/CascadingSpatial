package com.esri;

import cascading.flow.FlowException;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.OperatorContains;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.QuadTree;
import com.esri.shp.ShpHeader;
import com.esri.shp.ShpReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

/**
 */
public class SpatialDensity extends BaseOperation<SpatialContext> implements Function<SpatialContext>
{
    public static final String KEY_SHP = "com.esri.shp";

    private static final Logger LOG = LoggerFactory.getLogger(SpatialDensity.class);

    public SpatialDensity()
    {
        // Expecting two arguments and declare field 'ORIGID'
        super(2, new Fields("ORIGID"));
    }

    public SpatialDensity(final Fields fieldDeclaration)
    {
        super(2, fieldDeclaration);
    }

    @Override
    public void prepare(
            final FlowProcess flowProcess,
            final OperationCall<SpatialContext> operationCall)
    {
        if (operationCall.getContext() == null)
        {
            operationCall.setContext(new SpatialContext());
        }
        final SpatialContext spatialContext = operationCall.getContext();

        spatialContext.contains = OperatorContains.local();
        spatialContext.result = new Tuple(0);

        final Fields argumentFields = operationCall.getArgumentFields();
        spatialContext.fieldX = argumentFields.get(0);
        spatialContext.fieldY = argumentFields.get(1);
        try
        {
            final DataInputStream dataInputStream;
            if (flowProcess instanceof HadoopFlowProcess)
            {
                final HadoopFlowProcess hadoopFlowProcess = (HadoopFlowProcess) flowProcess;
                final JobConf jobConf = hadoopFlowProcess.getJobConf();
                final Path path = new Path(jobConf.get(KEY_SHP));
                dataInputStream = path.getFileSystem(jobConf).open(path);
            }
            else
            {
                final String name = flowProcess.getProperty(KEY_SHP).toString();
                dataInputStream = new DataInputStream(new FileInputStream(name));
            }
            try
            {
                indexShapefile(spatialContext, dataInputStream);
            }
            finally
            {
                dataInputStream.close();
            }
        }
        catch (IOException e)
        {
            throw new FlowException(e);
        }
    }

    private void indexShapefile(
            final SpatialContext spatialContext,
            final DataInputStream dataInputStream) throws IOException
    {
        int element = 0;
        final ShpReader shpReader = new ShpReader(dataInputStream);
        final ShpHeader shpHeader = shpReader.getHeader();
        spatialContext.extent = new Envelope2D(shpHeader.xmin, shpHeader.ymin, shpHeader.xmax, shpHeader.ymax);
        spatialContext.quadTree = new QuadTree(spatialContext.extent, 8); // TODO - make depth configurable
        final Envelope2D boundingBox = new Envelope2D();
        while (shpReader.hasMore())
        {
            final Polygon polygon = shpReader.readPolygon();
            spatialContext.contains.accelerateGeometry(polygon, null, Geometry.GeometryAccelerationDegree.enumHot);
            polygon.queryEnvelope2D(boundingBox);
            spatialContext.quadTree.insert(element++, boundingBox);
            spatialContext.list.add(polygon);
        }
        spatialContext.quadTreeIterator = spatialContext.quadTree.getIterator();
    }

    @Override
    public void operate(
            final FlowProcess flowProcess,
            final FunctionCall<SpatialContext> functionCall)
    {
        final SpatialContext spatialContext = functionCall.getContext();

        final TupleEntry arguments = functionCall.getArguments();
        final double x = arguments.getDouble(spatialContext.fieldX);
        final double y = arguments.getDouble(spatialContext.fieldY);

        if (spatialContext.extent.contains(x, y))
        {
            spatialContext.point.setXY(x, y);
            spatialContext.envelope2D.setCoords(x, y, x, y);
            spatialContext.quadTreeIterator.resetIterator(spatialContext.envelope2D, 0.000001); // TODO - make tolerance configurable
            int elementIndex = spatialContext.quadTreeIterator.next();
            while (elementIndex > -1)
            {
                final int listIndex = spatialContext.quadTree.getElement(elementIndex);
                final Polygon polygon = spatialContext.list.get(listIndex);
                if (spatialContext.contains.execute(polygon, spatialContext.point, null, null))
                {
                    final Tuple result = spatialContext.result;
                    result.set(0, listIndex);
                    functionCall.getOutputCollector().add(result);
                    break;
                }
                elementIndex = spatialContext.quadTreeIterator.next();
            }
        }
    }

    @Override
    public void cleanup(
            final FlowProcess flowProcess,
            final OperationCall<SpatialContext> operationCall)
    {
        operationCall.setContext(null);
    }
}
