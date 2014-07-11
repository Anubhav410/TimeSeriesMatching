package dtw_3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayDeque;
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
import org.hamcrest.internal.ArrayIterator;


public class Map extends  Mapper<LongWritable , Text , DoubleWritable, ArrayWritable> {
	static int INF = (int) 1e20;
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
		String[] temp1 = values.toString().split("[ \t\n]");
		int dLen = temp1.length;// length of each chunk of data and not the total size of it
		int m = 128;		//this is the length of the query

		double[] query = new double[m];
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


		ArrayIterator dataIterator = new ArrayIterator(data);



		double bsf;          /// best-so-far
		double[] t;
		double[] q;       /// data array and query array
		int[] order;          ///new order of the query
		double[] u, l, qo, uo, lo,tz,cb, cb1, cb2,u_d,l_d;


		double d = 0;
		long   i , j;
		double ex , ex2 , mean, std;
		int  r=-1;
		long loc = 0;
		double t1,t2;
		int kim = 0,keogh = 0, keogh2 = 0;
		double dist=0, lb_kim=0, lb_k=0, lb_k2=0;
		double[] buffer, u_buff, l_buff;
		Index[] Q_tmp;

		int EPOCH = 100000;

		//		m = Integer.parseInt(args[0]);

		//		double R = Double.parseDouble(args[1]);


		m = 128;
		double R = 0.1;

		if (R<=1)
			r = (int) Math.floor(R*m);
		else
			r = (int) Math.floor(R);


		double startTime = System.currentTimeMillis();


		q = new double[m];
		qo = new double[m];
		lo = new double[m];
		uo = new double[m];
		order = new int[m];

		Q_tmp = new Index[m];

		u = new double[m];
		l = new double[m];
		cb = new double[m];
		cb1 = new double[m];
		cb2 = new double[m];

		u_d = new double[m];
		l_d = new double[m];
		t = new double[2*m];

		tz = new double[m];
		buffer = new double[EPOCH];

		u_buff = new double[EPOCH];
		l_buff = new double[EPOCH];

		/// Read query file
		bsf = INF;
		bsf = checkIf();
		
//		System.out.println("Starts : " + bsf);
		i = 0;
		j = 0;
		ex = ex2 = 0;

		//		q = readFile(new File("Query.txt"), m);
		q = readQuery(128);

		ex = SUM(q);
		ex2 = SUM(SQR(q));
		mean = ex/m;
		std = ex2/m;

		std = Math.sqrt(std - mean*mean);

		for( i = 0 ; i < m ; i++ )
			q[(int)i] = (q[(int)i] - mean)/std;


		/// Create envelop of the query: lower envelop, l, and upper envelop, u
		MyPair myPair = lower_upper_lemire(q, m, r, l, u);

		l = myPair.one;
		u = myPair.two;

		/// Sort the query one time by abs(z-norm(q[i]))
		for( i = 0; i<m; i++)
		{
			//			System.out.println(q[(int) i]);
			Q_tmp[(int)i] = new Index(q[(int)i], (int)i);

		}

		Arrays.sort(Q_tmp);


		/// also create another arrays for keeping sorted envelop
		for( i=0; i<m; i++)
		{   
			int o = Q_tmp[(int) i].index;
			order[(int) i] = o;
			qo[(int) i] = q[o];
			uo[(int) i] = u[o];
			lo[(int) i] = l[o];
		}

		/// Initial the cummulative lower bound
		/*		for( i=0; i<m; i++)
		{   
			cb[(int) i]=0;
			cb1[(int) i]=0;
			cb2[(int) i]=0;
		}
		 */
		i = 0;          /// current index of the data in current chunk of size EPOCH
		j = 0;          /// the starting index of the data in the circular array, t
		ex = ex2 = 0;
		Boolean done = false;
		int it=0, ep=0, k=0;
		long  I;    /// the starting index of the data in current chunk of size EPOCH


		//		System.out.println("mid time: " + (System.currentTimeMillis() - startTime));

		while(!done)
		{
			/// Read first m-1 points
			ep=0;
			if (it==0)
			{   
				for(k=0; k<m-1; k++)
					if (dataIterator.hasNext())
					{
						d = (double) dataIterator.next();
						buffer[k] = d;

					}
			}
			else
			{   
				for(k=0; k<m-1; k++)
					buffer[k] = buffer[EPOCH-m+1+k];
			}

			/// Read buffer of size EPOCH or when all data has been read.
			ep=m-1;
			while(ep<EPOCH)
			{   
				if (!dataIterator.hasNext())
				{
					break;
				}
				d = (double) dataIterator.next();
				buffer[ep] = d;
				ep++;
			}

			/// Data are read in chunk of size EPOCH.
			/// When there is nothing to read, the loop is end.
			if (ep<=m-1)
			{
				done = true;
			} 
			else
			{   
				MyPair p = lower_upper_lemire(buffer, ep, r, l_buff, u_buff);
				l_buff = p.one;
				u_buff = p.two;

				/// Just for printing a dot for approximate a million point. Not much accurate.
				if (it%(1000000/(EPOCH-m+1))==0)
				{	
//					System.out.println(".");
				}			
				/// Do main task here..
				ex=0; ex2=0;
				for(i=0; i<ep; i++)
				{
					/// A bunch of data has been read and pick one of them at a time to use
					d = buffer[(int) i];

					/// Calcualte sum and sum square
					ex += d;
					ex2 += d*d;

					/// t is a circular array for keeping current data
					t[(int) (i%m)] = d;

					/// Double the size for avoiding using modulo "%" operator
					t[(int) ((i%m)+m)] = d;

					/// Start the task when there are more than m-1 points in the current chunk
					if( i >= m-1 )
					{
						mean = ex/m;
						std = ex2/m;
						std = Math.sqrt(std-mean*mean);

						/// compute the start location of the data in the current circular array, t
						j = (i+1)%m;
						/// the start location of the data in the current chunk
						I = i-(m-1);

						/// Use a constant lower bound to prune the obvious subsequence
						lb_kim = lb_kim_hierarchy(t, q, (int)j, m, mean, std, bsf);

						if (lb_kim < bsf)
						{
							/// Use a linear time lower bound to prune; z_normalization of t will be computed on the fly.
							/// uo, lo are envelop of the query.
							MyPair myPair1 = lb_keogh_cumulative(order, t, uo, lo, cb1, (int)j, m, mean, std, bsf);

							cb1 = myPair1.one;
							lb_k = myPair1.ddd;

							if (lb_k < bsf)
							{
								/// Take another linear time to compute z_normalization of t.
								/// Note that for better optimization, this can merge to the previous function.
								for(k=0;k<m;k++)
								{  
									tz[k] = (t[(int) (k+j)] - mean)/std;
								}

								/// Use another lb_keogh to prune
								/// qo is the sorted query. tz is unsorted z_normalized data.
								/// l_buff, u_buff are big envelop for all data in this chunk
								MyPair myPair2 =  lb_keogh_data_cumulative(order, tz, qo, cb2, l_buff, u_buff, m, mean, std, bsf, (int)I);
								cb2 = myPair2.one;
								lb_k2 = myPair2.ddd;


								if (lb_k2 < bsf)
								{
									/// Choose better lower bound between lb_keogh and lb_keogh2 to be used in early abandoning DTW
									/// Note that cb and cb2 will be cumulative summed here.
									if (lb_k > lb_k2)
									{
										cb[m-1]=cb1[m-1];
										for(k=m-2; k>=0; k--)
											cb[k] = cb[k+1]+cb1[k];
									}
									else
									{
										cb[m-1]=cb2[m-1];
										for(k=m-2; k>=0; k--)
											cb[k] = cb[k+1]+cb2[k];
									}

									/// Compute DTW and early abandoning if possible
									dist = dtw(tz, q, cb, m, r, bsf);

									//								System.out.println("dist : " + dist);

									if( dist < bsf )
									{   /// Update bsf
										/// loc is the real starting location of the nearest neighbor in the file

										bsf = dist;
										loc = (it)*(EPOCH-m+1) + i-m+1;
										//									System.out.println("I am inside this IF ststement at line 276.....loc : " + loc+ "   i : " + i + " it : " + it);									
									}
								} 
								else
									++keogh2;
							} 
							else
								++keogh;
						} 
						else
							++kim;

						/// Reduce obsolute points from sum and sum square
						ex -= t[(int) j];
						ex2 -= t[(int) j]*t[(int) j];
					}
				}

				/// If the size of last chunk is less then EPOCH, then no more data and terminate.
				if (ep<EPOCH)
					done=true;
				else
				{
					++it;
					//				System.out.println("Awesome i am here....." + it);

				}
			}
		}
		i = (it)*(EPOCH-m+1) + ep;

//		System.out.println("Location : " + (loc + (mapper-1)*200000));
//		System.out.println("Data Scanned :  " + i);
//		System.out.println("Distance : " + Math.sqrt(bsf));
		double stopTime = System.currentTimeMillis();
		double totalTime = stopTime - startTime;

//		System.out.println("Pruned by LB_Kim    : "+ ((double) kim / i)*100);
//		System.out.println("Pruned by LB_Keogh  : "+ ((double) keogh / i)*100);
//		System.out.println("Pruned by LB_Keogh2 : "+ ((double) keogh2 / i)*100);
//		System.out.println("DTW Calculation     : " + (100-(((double)kim+keogh+keogh2)/i*100)));


//		System.out.println((int)totalTime/1000 + " seconds " + totalTime%1000 + "  milliseconds");





		DoubleWritable[] tuple = new DoubleWritable[2];
		tuple[0] = new DoubleWritable(loc + (200000*(mapper-1)));
		tuple[1] = new DoubleWritable( Math.sqrt(bsf));
		//				System.out.println("mapp : " + tuple[0].toString());

		DoubleArrayWritable tt = new DoubleArrayWritable();
		tt.set(tuple);


		double tttt = checkIf();
		if(tttt > bsf)
			writeBSF(bsf);



		context.write(new DoubleWritable(reducerKey) , tt);//new tuple(loc , Math.sqrt(dist)));
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


	public static double min(double x , double y)
	{
		return ((x)<(y)?(x):(y));
	}
	public static double max(double x , double y)
	{
		return ((x)>(y)?(x):(y));
	}


	public static double dist(double x , double y)
	{
		return (x-y)*(x-y);
	}

	static MyPair lower_upper_lemire(double[] t, int len, int r, double[] l, double[] u)
	{

		ArrayDeque<Integer> dl = new ArrayDeque<>();
		ArrayDeque<Integer> du = new ArrayDeque<>();

		du.add(0);
		dl.add(0);


		for (int i = 1; i < len; i++)
		{
			if (i > r)
			{
				u[i-r-1] = t[du.getFirst()];
				l[i-r-1] = t[dl.getFirst()];
			}
			if (t[i] > t[i-1])
			{
				du.pollLast();
				while (!du.isEmpty() && t[i] > t[du.getLast()])
					du.pollLast();
			}
			else
			{
				dl.pollLast();
				while (!dl.isEmpty() && t[i] < t[dl.getLast()])
					dl.pollLast();
			}
			du.add(i);
			dl.add(i);
			if (i == 2 * r + 1 + du.getFirst())
				du.pollFirst();
			else if (i == 2 * r + 1 + dl.getFirst())
				dl.pollFirst();
		}
		for (int i = len; i < len+r+1; i++)
		{
			u[i-r-1] = t[du.getFirst()];
			l[i-r-1] = t[dl.getFirst()];
			if (i-du.getFirst() >= 2 * r + 1)
				du.pollFirst();
			if (i-dl.getFirst() >= 2 * r + 1)
				dl.pollFirst();
		}

		return (new MyPair(l, u,0));
	}
	static double lb_kim_hierarchy(double[] t, double[] q, int j, int len, double mean, double std, double bsf ) //there is a default value here...pls have a look!!
	{
		/// 1 point at front and back
		double d, lb;
		double x0 = (t[j] - mean) / std;
		double y0 = (t[(len-1+j)] - mean) / std;
		lb = dist(x0,q[0]) + dist(y0,q[len-1]);
		if (lb >= bsf)   return lb;

		/// 2 points at front
		double x1 = (t[(j+1)] - mean) / std;
		d = min(dist(x1,q[0]), dist(x0,q[1]));
		d = min(d, dist(x1,q[1]));
		lb += d;
		if (lb >= bsf)   return lb;

		/// 2 points at back
		double y1 = (t[(len-2+j)] - mean) / std;
		d = min(dist(y1,q[len-1]), dist(y0, q[len-2]) );
		d = min(d, dist(y1,q[len-2]));
		lb += d;
		if (lb >= bsf)   return lb;

		/// 3 points at front
		double x2 = (t[(j+2)] - mean) / std;
		d = min(dist(x0,q[2]), dist(x1, q[2]));
		d = min(d, dist(x2,q[2]));
		d = min(d, dist(x2,q[1]));
		d = min(d, dist(x2,q[0]));
		lb += d;
		if (lb >= bsf)   return lb;

		/// 3 points at back
		double y2 = (t[(len-3+j)] - mean) / std;
		d = min(dist(y0,q[len-3]), dist(y1, q[len-3]));
		d = min(d, dist(y2,q[len-3]));
		d = min(d, dist(y2,q[len-2]));
		d = min(d, dist(y2,q[len-1]));
		lb += d;

		return lb;
	}

	static MyPair lb_keogh_cumulative(int[] order, double[] t, double[] uo, double[] lo, double[]cb, int j, int len, double mean, double std, double best_so_far) //there is default value here...pls take care
	{
		double lb = 0;
		double x, d;

		for (int i = 0; i < len && lb < best_so_far; i++)
		{
			x = (t[(order[i]+j)] - mean) / std;
			d = 0;
			if (x > uo[i])
				d = dist(x,uo[i]);
			else if(x < lo[i])
				d = dist(x,lo[i]);
			lb += d;
			cb[order[i]] = d;
		}


		return new MyPair(cb, null, lb);
	}

	static MyPair lb_keogh_data_cumulative(int[] order, double[] tz, double[] qo, double[] cb, double[] l, double[] u, int len, double mean, double std, double best_so_far, int ABC) // there is a default value in here...pls take care
	{
		double lb = 0;
		double uu,ll,d;

		for (int i = 0; i < len && lb < best_so_far; i++)
		{
			uu = (u[order[i] +ABC]-mean)/std;
			ll = (l[order[i] + ABC]-mean)/std;
			d = 0;
			if (qo[i] > uu)
				d = dist(qo[i], uu);
			else
			{   if(qo[i] < ll)
				d = dist(qo[i], ll);
			}
			lb += d;
			cb[order[i]] = d;
		}
		return new MyPair(cb, null, lb);
	}


	static double dtw(double[] A, double[] B, double[] cb, int m, int r, double bsf)	//a default param...pls take care
	{

		double[] cost;
		double[] cost_prev;
		double[] cost_tmp;
		int i,j,k;
		double x,y,z,min_cost;

		/// Instead of using matrix of size O(m^2) or O(mr), we will reuse two array of size O(r).
		cost = new double[2*r+1];

		for(k=0; k<2*r+1; k++)    
			cost[k]= INF;

		cost_prev = new double[2*r + 1];

		for(k=0; k<2*r+1; k++)    cost_prev[k] = INF;

		for (i=0; i<m; i++)
		{
			k = (int) max(0,r-i);
			min_cost = INF;		//INFINITY value

			for(j=(int) max(0,i-r); j<=min(m-1,i+r); j++, k++)
			{
				/// Initialize all row and column
				if ((i==0)&&(j==0))
				{
					cost[k]=dist(A[0],B[0]);
					min_cost = cost[k];
					continue;
				}

				if ((j-1<0)||(k-1<0))     y = INF;
				else                      y = cost[k-1];
				if ((i-1<0)||(k+1>2*r))   x = INF;
				else                      x = cost_prev[k+1];
				if ((i-1<0)||(j-1<0))     z = INF;
				else                      z = cost_prev[k];

				/// Classic DTW calculation
				cost[k] = min( min( x, y) , z) + dist(A[i],B[j]);

				/// Find minimum cost in row for early abandoning (possibly to use column instead of row).
				if (cost[k] < min_cost)
				{   min_cost = cost[k];
				}
			}

			/// We can abandon early if the current cummulative distace with lower bound together are larger than bsf
			if (i+r < m-1 && min_cost + cb[i+r+1] >= bsf)
			{
				return min_cost + cb[i+r+1];
			}

			/// Move current array to previous array.
			cost_tmp = cost;
			cost = cost_prev;
			cost_prev = cost_tmp;
		}
		k--;

		/// the DTW distance is in the last cell in the matrix of size O(m^2) or at the middle of our array.
		double final_dtw = cost_prev[k];
		return final_dtw;
	}

	private void writeBSF(double bsf) throws IOException {

		BufferedWriter bw=new BufferedWriter(new FileWriter(new File("C:/cygwin64/bsf.txt")));
		bw.write(bsf+" \n");
		bw.close();


	}

	//..............................................................................................................................		


	private double checkIf() throws IOException {
		BufferedReader br = null;
		try{
			br = new BufferedReader(new FileReader(new File("C:/cygwin64/bsf.txt")));

			String ss = br.readLine();
			if(!ss.isEmpty())
			{
				double dd = Double.parseDouble(ss);
				return dd;
			}
		}
		catch(Exception e){
			return 1e20; 
		}
		finally{
			if(br !=null)
				br.close();
		}
		return 1e20;
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



class MyPair
{
	double[] one;
	double[] two;
	double ddd;
	public MyPair(double[] x, double[] y, double d)
	{
		one = x;
		two = y;
		ddd = d;
	}

}

