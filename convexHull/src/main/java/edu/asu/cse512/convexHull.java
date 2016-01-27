package edu.asu.cse512;

import java.util.Iterator;
import java.util.ArrayDeque;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.io.WKTReader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class convexHull 
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
		SavetoHDFS(hull, context, outputFile);
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
	
	public static void SavetoHDFS(Geometry hull, JavaSparkContext context, String outputFile) 
	{
		Coordinate[] coordinates = hull.getCoordinates();
		ArrayList<String> hullPointList = new ArrayList<String>();
		for (Coordinate coordinate : coordinates) 
		{
			hullPointList.add(coordinate.x + ", " + coordinate.y);
		}
		JavaRDD<String> hullRDD = context.parallelize(hullPointList).coalesce(1);
		hullRDD.saveAsTextFile(outputFile);
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
				Geometry pointUnion=geoQueue.pollFirst();
				for(int i=1;i<geoQueue.size();i++)
					pointUnion = pointUnion.union(geoQueue.pollFirst().convexHull());
	
				hull.add(pointUnion);
			}
			return hull;
		}
	}
	
	public static void main(String[] args)
	{
		if(args.length < 2)
		{
			System.out.println("Invalid Arguments!");
			return;
		}
		SparkConf conf = new SparkConf().setAppName("Group18-ConvexHull").setMaster("local[1]");
		//SparkConf conf = new SparkConf().setAppName("Group18-ConvexHull").setMaster("spark://192.168.0.133:7077").setJars(new String[] {"/home/anand/convexHull-0.1.jar", "jts.jar"});
		JavaSparkContext context = new JavaSparkContext(conf);
		convexHull.ConstructConvexHull(context, args[0], args[1]);
	}
}
