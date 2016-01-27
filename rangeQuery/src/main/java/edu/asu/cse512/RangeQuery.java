package edu.asu.cse512;

import java.io.Serializable;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;

class Window implements Serializable {
	private static final long serialVersionUID = 1L;
	int lineid;
	 double point1x;
	 double point1y;
	 double point2x;
	 double point2y;
public Window(int identifier,double[] cord_array) {
	
	lineid = identifier;
	point1x = calculateMin(cord_array[0], cord_array[2]);
	point1y = calculateMax(cord_array[1], cord_array[3]);
	point2x = calculateMax(cord_array[0], cord_array[2]);
	point2y = calculateMin(cord_array[1], cord_array[3]);
}

public double calculateMin(double a,double b)
{
	if(a>b)
		return b;
	else
		return a;
}
public double calculateMax(double a,double b)
{
	if(a<b)
		return b;
	else
		return a;
}

	public boolean containsPoint(Window win) {

		boolean isNull = (win==null)? false : true;
		if(!isNull)
			return false;
		boolean check1=point1x <= win.point1x;
		boolean check2=point1y >= win.point1y;
		boolean check3=point2x >= win.point2x;
		boolean check4=point2y <= win.point2y;
		
		return check1 && check2 && check3 && check4;
	}

	@Override
	public String toString() {
		return Integer.toString(lineid);
	}

	public int getId() {
		return lineid;
	}
}
public class RangeQuery {
	
	public static void range(JavaSparkContext context, String ptsIp,
			String rectIp, String resOp) {
		
		JavaRDD<String> ptsFile,rectFile;
		JavaRDD<Window> points,rect,fin_res;
		final Broadcast<Window> window;
		ptsFile = context.textFile(ptsIp);
		rectFile = context.textFile(rectIp);
		points = ptsFile.map(new Function<String, Window>() {


			private static final long serialVersionUID = 1L;

			public Window call(String v1) throws Exception {
				List<String> colSplit;
				colSplit= Arrays.asList(v1.split(","));
				int ColNo,checker=0,hashcode;
				double p1x, p1y;
				hashcode =0;
				p1y = 0;
				p1x = 0;
				ColNo=colSplit.size();
				checker = (ColNo < 3)?0:1;
				if (checker==0) {
					return null;
				}
				hashcode = Integer.parseInt(colSplit.get(0).toString());
				p1x = Double.parseDouble(colSplit.get(1).toString());
				p1y = Double.parseDouble(colSplit.get(2).toString());
				double[] cord_array = new double[4];

				cord_array[0]=p1x;
				cord_array[2]=p1y; //single point rectangle
				cord_array[1]=p1x;
				cord_array[3]=p1y;
				
				Window pts = new Window(hashcode, cord_array);
				return pts;
			}
		});
	    rect = rectFile.map(new Function<String, Window>() {


			private static final long serialVersionUID = 1L;

			public Window call(String v1) throws Exception {
				List<String> col;
				col= Arrays.asList(v1.split(","));
				int colno=col.size(),hashcode = -1,checker=0;
				double p1x = 0.0, p1y = 0.0, p2x = 0.0, p2y = 0.0;
				checker =(colno < 4)? 0:1;
				if (checker == 0) {
					return null;
				}
				
				p1x = Double.parseDouble(col.get(0).toString());
				p1y = Double.parseDouble(col.get(1).toString());
				p2x = Double.parseDouble(col.get(2).toString());
				p2y = Double.parseDouble(col.get(3).toString());
				
				double[] cord_array= new double[4];
				cord_array[0]=p1x;
				cord_array[1]=p1y;
				cord_array[2]=p2x;
				cord_array[3]=p2y;
				
				Window rec = new Window(hashcode, cord_array);
				return rec;
			}
		});
		 window=context.broadcast(rect.first());
	    fin_res = points
				.filter(new Function<Window, Boolean>() {
				
					private static final long serialVersionUID = 1L;

							public Boolean call(Window v) {
											
												return window.value().containsPoint(v);
											
					}
				});

		fin_res.coalesce(1).saveAsTextFile(resOp);
		context.close();
	}
	public static void main(String[] args)
    {
		if(args.length < 3)
		{
        		System.out.println("Invalid Arguments!");
        		return;
		}
        SparkConf conf = new SparkConf().setAppName("Group18-RangeQuery");
        JavaSparkContext context = new JavaSparkContext(conf);
      
        RangeQuery.range(context, args[0],args[1],args[2]);
    }
}