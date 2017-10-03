package mapReduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MapReducePivot {
	public static class PivotMap extends Mapper<Object, Text, Text, IntWritable>{
		public void map(int key, Text value, Context context) throws IOException, InterruptedException {
			String tab[] = new value.split(",");
			int i = 0;
			for(i=0;i=tab.;i++)
				context.write(i, tab[i]);
		}
	}




	public static class PivotReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(int key, Text value, Context context) throws IOException, InterruptedException {
			String out[] = new value.split(",");
			int i = 0;
			for(String V : value) {
				out += V+";";
			}
			context.write(i, out[i]);
		}
	}
}
