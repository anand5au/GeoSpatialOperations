package edu.asu.cse512;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.*;

import com.vividsolutions.jts.geom.Coordinate;

import scala.Tuple2;

import java.io.Serializable;

class PointClass implements Serializable
{
private static final long serialVersionUID = 1L;
double xCord, yCord;

public PointClass(Coordinate c)
{
xCord = c.x;
yCord = c.y;
this.id = hashCode();

}
private final int id;

public double getxCord() {
return xCord;
}

public double getyCord() {
return yCord;
}

public double PointDistance(PointClass point) {

return (xCord - point.xCord) * (xCord - point.xCord) + (yCord - point.yCord) * (yCord - point.yCord);
}
public int getId() {
    return id;
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
public PairOfPoints(PointClass a, PointClass b) {
    this.point1 = a;
    this.point2 = b;
    this.distance = a.PointDistance(b);
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


public class ClosestPair implements Serializable {
    
	private static final long serialVersionUID = 1L;

	public static void main(String[] args){
        if(args.length < 2)
            System.out.println("Invalid Arguments!");
            
        
        SparkConf conf = new SparkConf().setAppName("Group18-ClosestPair");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<PointClass> points=readHDFSPointFile(context,args[0]);
        List<PointClass> clist=points.collect();
        ClosestPair.closestPoints(context, clist,args[1]);
        
    }
     public static JavaRDD<PointClass> readHDFSPointFile(JavaSparkContext context,String input){
            JavaRDD<String> lines=context.textFile(input);
            return lines.map(PARSE_POINT_MAP);
        }
     
     private static final Function<String, PointClass> PARSE_POINT_MAP = new Function<String, PointClass>() {
            private static final long serialVersionUID = 1L;
     public PointClass call(String arg) throws Exception {
            // TODO Auto-generated method stub
            String[] term=arg.split(",");
            assert term.length==2;
            Coordinate c=new Coordinate();
            c.x=Double.parseDouble(term[0]);
            c.y=Double.parseDouble(term[1]);
            return new PointClass(c);
        }

    };
   
    public static void closestPoints(JavaSparkContext context,
            List<PointClass> points,String output) {
        JavaRDD<PointClass> pointsRDD;
        pointsRDD=context.parallelize(points);
        final Broadcast<List<PointClass>> broadcastPoints;
        broadcastPoints=context.broadcast(pointsRDD.collect());


        JavaRDD<PairOfPoints> pairs = pointsRDD
                .map(new Function<PointClass, PairOfPoints>() {
                    private static final long serialVersionUID = 1L;
                    
                    
                    
                    public PairOfPoints call(PointClass point) {
                        PairOfPoints closest = null;
                        for (PointClass pt : broadcastPoints.value()) {
                            
                            if (point.getId() >= pt.getId())
                                continue;
                            if (closest == null
                                    || point.PointDistance(pt) < closest.PointDistance()) {
                            
                                closest = new PairOfPoints(point, pt);
                            }
                        }

                        return closest;
                    }
                });

        
        PairOfPoints final_result=pairs.reduce(new Function2<PairOfPoints, PairOfPoints, PairOfPoints>() {
           private static final long serialVersionUID = 1L;

           public PairOfPoints call(PairOfPoints point1, PairOfPoints point2) {
               
              Boolean flag=true;
               Boolean flag2=true;
               if(point1!=null)
                   flag=false;
               if(point2!=null)
                   flag2=false;
               if (flag && flag2)
                   return null;
               if (flag)
                   return point2;
               if (flag2)
                   return point1;
               return point1.PointDistance() < point2.PointDistance() ? point1 : point2;
           }
       }
);
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

}