package ed_2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import sun.awt.VerticalBagLayout;


public class Map extends  Mapper<LongWritable , Text , DoubleWritable, ArrayWritable> {

	int mapper = 0;
	int m = 128;
	int totInp = 0;
	int reducerKey = 501;
	public static double distance(double[] Q,double[] T,int j,int m,double mean,double std,int[] order,double bsf)
	{

		double sum_ = 0;
		for( int i = 0 ; i < m ; i++)
		{

			if(sum_ > bsf)
			{
				break;
			}
			else
			{
				double x = (T[order[i]+j]-mean)/std;
				sum_ += (x - Q[i])*(x - Q[i]);
			}
		}
		return sum_;

	}

	public static double[]  SQR(double[] arr)
	{
		double[] res = new double[arr.length] ;

		int i = 0;
		for(double x: arr)
		{
			double y = arr[i];
			res[i++] = y*y;

		}

		return res;
	}

	public static double  SUM(double[] arr)
	{
		double res = 0;
		for(double x: arr)
			res += x;

		return res;
	}


	public void map(LongWritable key, Text values,Context context)
			throws IOException, InterruptedException 
			{

		mapper++;	

		//		System.out.println("Mapper " + mapper + " :  KEY : " + key);//values.toString().split("[ \t\n]").length);

		int total = 1000000;
		int qLen = 128 ; //length of query
		String[] temp1 = values.toString().split("[ \t\n]");
		int dLen = temp1.length;// length of each chunk of data and not the total size of it
		int m = qLen;

		double[] query = new double[qLen];
		double[]  data;

		query = readQuery(128);

		data = new double[dLen];
		int dp = 0;

		for (int i = 0; i < temp1.length; i++) {
			String d = temp1[i];
			if(!d.isEmpty()){
			data[dp++] = Double.parseDouble(d);
			totInp++;
			}
		}

		//..............................................................................................................................
		double bsf = 1e20;		//best so far , infinity for starting
		double	ex = SUM(query);
		double	ex1 = SUM(SQR(query));
		double mean = ex/m;
		double	std = ex1/m;

		std = Math.sqrt(std - mean*mean);

		int i = 0 ;
		for(i =0 ; i< m ; i++)
		{
			double X = query[i];

			query[i] = (X-mean)/std;
		}


		Index2[] Q_tmp = new Index2[m];

		i = 0 ;
		for(double X : query)
		{
			Q_tmp[i] = new Index2(X,i);
			i++;
		}

		Arrays.sort(Q_tmp);

		int[] order = new int[m]; //this array hoolds the proper ordering of the Index2es in the queryay

		for(int j = 0;j < m ; j++)
		{
			query[j] = Q_tmp[j].val;
			order[j] =  Q_tmp[j].Index2;
		}

		//-----------------------------------------------------------------------------------------------------------------------------------------

		double[] T = new double[2*m];
		double dist = 0;
		int j = 0;
		double ex2 = 0;

		int loc = 0;
		i = 0;
		ex = 0;



		for(i =0; i<dLen ; i++)
		{
			//				System.out.println("i : " + i);
			ex += data[i];
			ex2 += data[i]*data[i];
			T[i%m] = data[i];
			T[(i%m)+m] = data[i];

			if( i >= m-1 )
			{
				j = (i+1)%m;

				mean = ex/m;
				std = ex2/m;
				double temp = std - mean*mean;
				std = Math.sqrt(temp);

				dist = distance(query,T,j,m,mean,std,order,bsf);
				//					System.out.println("dist : " +dist);

				if( dist < bsf )
				{
					bsf = dist;
					loc = i-m+1;
				}
				ex -= T[j];
				ex2 -= T[j]*T[j];
			}

		}
		//		System.out.println("location : "+ (loc +(mapper-1)*200000) );
		//		System.out.println("locationd,mfjfd : "+ (loc ));
		//		System.out.println("distance : "+ Math.sqrt(dist));


		DoubleWritable[] tuple = new DoubleWritable[2];
		tuple[0] = new DoubleWritable(loc + (200000*(mapper-1)));
		tuple[1] = new DoubleWritable( Math.sqrt(bsf));
//				System.out.println("mapp : " + tuple[0].toString());

			DoubleArrayWritable tt = new DoubleArrayWritable();
			tt.set(tuple);
//		System.out.println("df,lmknf" + tt.get()[0] + "..." );//tt.readFields(null));

		
		context.write(new DoubleWritable(reducerKey) , tt);//new tuple(loc , Math.sqrt(dist)));
			}
	//..............................................................................................................................		


	private double[] readQuery(int qLen) throws FileNotFoundException 
	{
		double [] temp = new double[qLen];
//		Scanner sc  = new Scanner(new File("C:/cygwin64/query.txt"));
		Scanner sc  = new Scanner(new File("C:/cygwin64/query.txt"));
		int dd = 0;
		while(sc.hasNextDouble())
		{
			temp[dd++] = sc.nextDouble();

		}
		return temp;
	}


}



class Index2 implements Comparable<Index2>
{
	public Index2(double v , int i) 
	{
		val = v ;
		Index2 = i;
	}
	double val ;
	int Index2;
	@Override
	public int compareTo(Index2 o) {
		double temp = val - o.val;
		if(temp > 0)
			return 1;
		else if (temp < 0)
			return -1;
		else
			return 0;
	}
}	

