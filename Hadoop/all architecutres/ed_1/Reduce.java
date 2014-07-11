package ed_1;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reduce extends Reducer<Text,DoubleWritable,DoubleWritable,DoubleWritable> {


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



	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws 
	IOException, InterruptedException 
	{ 

		int total = 1000000;
		int qLen = 128 ; //length of query
		int dLen = 500000;// length of each chunk of data and not the total size of it
		int m = qLen;

		int temp1 =  (total-dLen)/(dLen-qLen);
		int temp2 = (temp1)*(dLen-qLen) + dLen;
		int lastChunkSize = total - temp2;
		int lastChunkNum = temp2 / (dLen - qLen);
		double[] query = new double[qLen];
		double[]  data;

		query = readQuery(128);

		if(key.toString().matches("data"+(lastChunkNum+1)+".*"))
		{
			dLen = lastChunkSize + qLen;
		}
		// 			*/ 
		data = new double[dLen];
		int dp = 0;
		while(values.iterator().hasNext())
		{
			data[dp++]  = values.iterator().next().get();
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


		Index[] Q_tmp = new Index[m];

		i = 0 ;
		for(double X : query)
		{
			Q_tmp[i] = new Index(X,i);
			i++;
		}

		Arrays.sort(Q_tmp);
		//at this point above the Array of Index class is sorted!!!!!! 
		//Implemnetd a Comparable Interface to do the comparision of Dpuble Values

		int[] order = new int[m]; //this array hoolds the proper ordering of the indexes in the queryay


		for(int j = 0;j < m ; j++)
		{
			query[j] = Q_tmp[j].val;
			order[j] =  Q_tmp[j].index;
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


		//			long stop = System.currentTimeMillis();
		//		System.out.println((stop - start) + "  miliseconds");
		Pattern p = Pattern.compile("([0-9]+)");
		Matcher m1 = p.matcher(key.toString());	
		m1.find();
		int mmm = Integer.parseInt(m1.group(1));
		System.out.println("mmm : " + mmm);

		System.out.println("location : "+ ( loc + dLen*(mmm-1)));
		System.out.println("distance : "+ Math.sqrt(bsf));

		context.write(new DoubleWritable(loc + dLen*(mmm-1)), new DoubleWritable( Math.sqrt(bsf)));

	}
	//..............................................................................................................................		


	private double[] readQuery(int qLen) throws FileNotFoundException 
	{
		double [] temp = new double[qLen];
		Scanner sc  = new Scanner(new File("C:/cygwin64/query.txt"));
		int dd = 0;
		while(sc.hasNextDouble())
		{
			temp[dd++] = sc.nextDouble();

		}
		return temp;
	}


}



class Index implements Comparable<Index>
{
	public Index(double v , int i) 
	{
		val = v ;
		index = i;
	}
	double val ;
	int index;
	@Override
	public int compareTo(Index o) {
		double temp = val - o.val;
		if(temp > 0)
			return 1;
		else if (temp < 0)
			return -1;
		else
			return 0;
	}
}	
