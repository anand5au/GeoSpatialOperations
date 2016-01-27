package edu.asu.cse512;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.WKTReader;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
class convexHull 
{
	public final static int Part_Count = 100;
	public static Geometry ConstructConvexHull(JavaSparkContext context, String inputFile, String outputFile)
	{
		JavaRDD<String> coordinate = context.textFile(inputFile);
		JavaRDD<Geometry> geometryCoordinate = coordinate.map(new LoadPoints());
		geometryCoordinate = geometryCoordinate.repartition(Part_Count);
		JavaRDD<Geometry> hullRDD = geometryCoordinate.mapPartitions(new ComputeConvexHull());
		Geometry hull = hullRDD.reduce( new Function2<Geometry, Geometry, Geometry>() 
								{
									private static final long serialVersionUID = 1L;
									public Geometry call(Geometry g1, Geometry g2) throws Exception 
									{
										return g1.union(g2);
									}
								});
		hull = ConvertPolygonToPoints(hull.convexHull());
		return hull;
	}
	
	static final class LoadPoints implements Function<String, Geometry> 
	{
		private static final long serialVersionUID = 1L;
		public Geometry call(String coordinate)
		{
			String[] coordinates = coordinate.split(",");
			if(coordinates.length == 2)
			{
				try
				{
					String wellKnownText = String.format("POINT (%.10f %.10f)", Double.parseDouble(coordinates[0]),
																		 		Double.parseDouble(coordinates[1]));
					return new WKTReader().read(wellKnownText);
				}
				catch(Exception ex) 
				{ 
					System.out.println(ex);
				}
			}
			return null;
		}
	}
	
	public final static Geometry ConvertPolygonToPoints(Geometry polygon)
	{
		Coordinate[] coordinates = polygon.getCoordinates();
		Geometry points = null;
		
		for(Coordinate c : coordinates)
		{
			Geometry tempPoint = null;
			try 
			{
				tempPoint = new WKTReader().read(String.format("POINT (%.10f %.10f)",
																		c.x, c.y));
			} 
			catch (Exception ex) 
			{
				System.out.println(ex);
			}

			points = (points == null) ? tempPoint : points.union(tempPoint);
		}
		return points;
	}
	
	
	@SuppressWarnings("serial")
	static final class ComputeConvexHull implements FlatMapFunction<Iterator<Geometry>, Geometry>
	{

		public Iterable<Geometry> call(Iterator<Geometry> iterator)
		{
			ArrayDeque<Geometry> geoQueue = new ArrayDeque<Geometry>(); 
			ArrayDeque<Geometry> hull = new ArrayDeque<Geometry>();
			
			while(iterator.hasNext())
				geoQueue.add(iterator.next());
			
			if(!geoQueue.isEmpty())
			{
				//System.out.println("Geometry list is not empty");
				Geometry pointUnion=geoQueue.pollFirst();
				for(int i=1;i<geoQueue.size();i++)
					pointUnion = pointUnion.union(geoQueue.pollFirst().convexHull());
	
				hull.add(pointUnion);
			}
			return hull;
		}
	}
	
}
class PointClass implements Serializable
{	 
	private static final long serialVersionUID = 1L;
double xCord, yCord;
	
	public PointClass(Coordinate c) 
	{
		xCord = c.x;
		yCord = c.y;
	}
	public double getxCord() {
		return xCord;
	}

	public double getyCord() {
		return yCord;
	}

	public double PointDistance(PointClass point) {

		return (xCord - point.xCord) * (xCord - point.xCord) + (yCord - point.yCord) * (yCord - point.yCord);
	}
}

class PairOfPoints implements Serializable {
	static final long serialVersionUID = 1L;
	PointClass point1, point2;
	double distance;
	public PairOfPoints(Tuple2<PointClass, PointClass> pair) {
		if(pair !=null)
		{
			point1 = pair._1;
			point2 = pair._2;
			distance = point1.PointDistance(point2);
		}
	}
	
	public PointClass getpoint1() {
		return point1;
	}
	
	public PointClass getpoint2() {
		return point2;
	}
	
	public double PointDistance() {
		return distance;
	}
	
}

public class FarthestPair implements Serializable {
	private static final long serialVersionUID = 1L;
	

		
	
	public static void farthestPoints(JavaSparkContext context, String input,String output) {
		Geometry g = convexHull.ConstructConvexHull(context, input, output) ;//ConstructConvexHull(context, input);
		MultiPoint mp;
		mp= (MultiPoint) g;		
		JavaRDD<Coordinate> c;
		c = context.parallelize(Arrays.asList(mp.getCoordinates()));
		JavaRDD<PointClass> pts;
		pts= c.map(	new Function<Coordinate, PointClass>() {
			private static final long serialVersionUID = 1L;
			
			public PointClass call(Coordinate coord) {
				return new PointClass(coord);
			}
		});
		JavaRDD<PairOfPoints> pairs;
		pairs= pts.cartesian(pts).map(	new Function<Tuple2<PointClass, PointClass>, PairOfPoints>() {
			private static final long serialVersionUID = 1L;
			public PairOfPoints call(Tuple2<PointClass, PointClass> pair) {
				PairOfPoints obj;

				obj=new PairOfPoints(pair);
				return obj;
			}
		});
		
		PairOfPoints final_result;
		final_result=pairs.reduce(new Function2<PairOfPoints, PairOfPoints, PairOfPoints>() {
			private static final long serialVersionUID = 1L;
			
			public PairOfPoints call(PairOfPoints point1, PairOfPoints point2) {
				PairOfPoints temp;
				if(point1.PointDistance() > point2.PointDistance())
					temp = point1;
				else
					temp = point2;
				return temp;
			}
		});	
		PairOfPoints sortedPoints= sort(final_result);
		 
		JavaRDD<String> out;
		out = context.parallelize(Arrays.asList(sortedPoints.getpoint1().getxCord() +","+sortedPoints.getpoint1().getyCord()+ "\n"+sortedPoints.getpoint2().getxCord()+","+sortedPoints.getpoint2().getyCord()));
		out.saveAsTextFile(output);
		
	}

	private static PairOfPoints sort(PairOfPoints final_result) {
		
		PairOfPoints temp = new PairOfPoints(null);
		
		if(final_result.getpoint1().getxCord() > final_result.getpoint2().getxCord())
		{
				temp.point2 = final_result.point1;
				temp.point1 = final_result.point2;
		}
		else if(final_result.getpoint1().getxCord() < final_result.getpoint2().getxCord())
		{
			temp.point1 = final_result.point1;
			temp.point2 = final_result.point2;
		}
		else //equal condition
		{
			if(final_result.getpoint1().getyCord() < final_result.getpoint2().getyCord())
			{
				temp.point1 = final_result.point1;
				temp.point2 = final_result.point2;
			}
			
			else if(final_result.getpoint1().getyCord() > final_result.getpoint2().getyCord())
			{
				temp.point2 = final_result.point1;
				temp.point1 = final_result.point2;
			}
		}
		return temp;
	}
	public static void main(String[] args)
    {
		if(args.length < 2)
		{		
			System.out.println("Invalid Arguments!");
			return;
		}
        SparkConf conf = new SparkConf().setAppName("Group18-FarthestPair");
        JavaSparkContext context = new JavaSparkContext(conf);
       FarthestPair.farthestPoints(context, args[0], args[1]);
    }
	
}


