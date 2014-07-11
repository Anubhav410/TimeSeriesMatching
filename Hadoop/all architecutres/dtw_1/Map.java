package dtw_1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Map extends  Mapper<LongWritable , Text , Text, DoubleWritable> {


	int m = 128;
	int lol = 0;
	int llll = 1;
	int chunkSize = 1000000;
	private int mapper = 0;
	public void map(LongWritable key, Text values,
			Context context) throws IOException, InterruptedException {

		
		mapper ++;
		System.out.println("Mapp Number : " + mapper);

		String filename = ((FileSplit) context.getInputSplit()).getPath().toString();
		String[] g = filename.split("/");
		filename = g[g.length-1];

		String line = values.toString();

		String keyLabel = "";



		if(filename.equalsIgnoreCase("data.txt"))
		{
			keyLabel = "data";
		}
		else if(filename.equalsIgnoreCase("query.txt"))
		{
			keyLabel = "query";
		}

		
		int cntr = 0;
		double buffer[] = new double[128];
		boolean bufferEmpty = true;
		for(String t : line.split(" "))
		{
			
			if(!bufferEmpty)
			{
				for(int i = 0 ; i< m ;i++)
				{
					context.write(new Text("data" + llll) , new DoubleWritable(buffer[i]));
					lol++;
				}
				
				bufferEmpty = true;
			}
			
			if(!t.isEmpty())
			{
				if(keyLabel.equals("data"))
				{
					context.write(new Text("data" + llll) , new DoubleWritable(Double.parseDouble(t)));
					lol++;
					if(lol>= chunkSize - m)
					{
						buffer[cntr++] = Double.parseDouble(t);
						if(cntr == m)
							cntr = 0;
	
					}
					if(lol >= chunkSize)
					{
						lol = 0;
						llll++;
						bufferEmpty = false;
					}
				}
				else
				{
					context.write(new Text("query" + llll) , new DoubleWritable(Double.parseDouble(t)));
				}
			}
		}

	}

}
