package dtw_3;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class Reduce extends Reducer<DoubleWritable,DoubleArrayWritable,DoubleWritable,DoubleWritable> {

	
	
		int red = 0;
		public void reduce(DoubleWritable key, Iterable<DoubleArrayWritable> values, Context context) throws 
		IOException, InterruptedException { 
	
			red++;
//			System.out.println("Reducer "+ red +" : " + key);
			Iterator<DoubleArrayWritable> hh = values.iterator();
			int loc = 0;
			double dist  = 1e20;
			
			while(hh.hasNext())
			{
				DoubleArrayWritable tmp = hh.next();
				
				double tempLoc = ((DoubleWritable)tmp.get()[0]).get();
				double tempDist = ((DoubleWritable)tmp.get()[1]).get();
				
				if(tempDist < dist)
				{
					dist = tempDist;
					loc = (int) tempLoc;
				}
		
			}
			
			
			context.write(new DoubleWritable(loc) , new DoubleWritable(dist));
			
		}
}	
