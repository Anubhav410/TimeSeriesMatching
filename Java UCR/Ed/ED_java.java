package Ed;
import java.util.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.*;

public class ED_java 
{

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

	public static double[] readFile(File file , int size) throws IOException
	{

		double[] tempArr1 = new double[size];

		Scanner sc = new Scanner(file);
		int i = 0 ;
		while(sc.hasNextDouble())
		{
			tempArr1[i++] = sc.nextDouble();
		}

		return tempArr1 ;
	}
	public static void main(String[] args) throws IOException 
	{


		long start = System.currentTimeMillis();

		File queryFile = new File("Query.txt");
		File dataFile = new File("Data.txt");

		int m = 128;
		int dataLen = 1000000;

		double[] queryArr = readFile(queryFile, m);
		double[] dataArr = readFile(dataFile, dataLen);


		System.out.println("length of dataArr : " + dataArr.length);
		double bsf = 1e20;		//best so far , infinity for starting
		double	ex = SUM(queryArr);
		double	ex1 = SUM(SQR(queryArr));
		double mean = ex/m;
		double	std = ex1/m;

		std = Math.sqrt(std - mean*mean);

		int i = 0 ;
		for(i =0 ; i< m ; i++)
		{
			double X = queryArr[i];
			
			queryArr[i] = (X-mean)/std;
		}


		Index[] Q_tmp = new Index[m];

		i = 0 ;
		for(double X : queryArr)
		{
			Q_tmp[i] = new Index(X,i);
			i++;
		}

		Arrays.sort(Q_tmp);
		//at this point above the Array of Index class is sorted!!!!!! 
		//Implemnetd a Comparable Interface to do the comparision of Dpuble Values

		int[] order = new int[m]; //this array hoolds the proper ordering of the indexes in the queryArray


		for(int j = 0;j < m ; j++)
		{
			queryArr[j] = Q_tmp[j].val;
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



		for(i =0; i<dataLen ; i++)
		{
			//			System.out.println("i : " + i);
			ex += dataArr[i];
			ex2 += dataArr[i]*dataArr[i];
			T[i%m] = dataArr[i];
			T[(i%m)+m] = dataArr[i];

			if( i >= m-1 )
			{
				j = (i+1)%m;

				mean = ex/m;
				std = ex2/m;
				double temp = std - mean*mean;
				std = Math.sqrt(temp);

				dist = distance(queryArr,T,j,m,mean,std,order,bsf);
				
				if( dist < bsf )
				{
					bsf = dist;
					loc = i-m+1;
				}
				ex -= T[j];
				ex2 -= T[j]*T[j];
			}

		}


		long stop = System.currentTimeMillis();
		System.out.println((stop - start) + "  miliseconds");
		System.out.println("location : "+ loc);
		System.out.println("distance : "+ Math.sqrt(dist));

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