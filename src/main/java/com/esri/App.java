package com.esri;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Hello spatial cascading !
 */
public class App
{
    public static void main(final String[] args) throws URISyntaxException
    {
        if (args.length != 3)
        {
            System.err.println("Arguments: [data.tsv] [polygon.shp] [output]");
            return;
        }

        final Fields inFields = new Fields("ID", "X", "Y").
                applyTypes(long.class, double.class, double.class);

        final Tap inTap = new Hfs(new TextDelimited(inFields, false, "\t"), args[0]);

        final Tap outTap = new Hfs(new TextDelimited(true, ","), args[2], SinkMode.REPLACE);

        final SpatialDensity spatialDensity = new SpatialDensity();

        Pipe pipe = new Each("start", new Fields("X", "Y"), spatialDensity);

        pipe = new GroupBy(pipe, spatialDensity.getFieldDeclaration());

        pipe = new Every(pipe, Fields.GROUP, new Count(new Fields("POPULATION")));

        final Properties properties = AppProps.appProps().
                setJarClass(App.class).
                buildProperties();

        properties.put(SpatialDensity.KEY_SHP, args[1]);

        final FlowConnector connector = new HadoopFlowConnector(properties);

        final Flow flow = connector.connect(inTap, outTap, pipe);

        flow.complete();
    }
}
