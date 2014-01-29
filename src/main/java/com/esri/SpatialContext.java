package com.esri;

import cascading.tuple.Tuple;
import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.OperatorContains;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.QuadTree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class SpatialContext implements Serializable
{
    public final List<Polygon> list = new ArrayList<Polygon>();
    public final Point point = new Point();
    public final Envelope2D envelope2D = new Envelope2D();
    public OperatorContains contains;
    public QuadTree quadTree;
    public QuadTree.QuadTreeIterator quadTreeIterator;
    public Comparable fieldX;
    public Comparable fieldY;
    public Envelope2D extent;
    public Tuple result;

    public SpatialContext()
    {
    }
}
