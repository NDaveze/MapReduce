import java.io.IOException;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PivotFinal {

  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, LongWritable, MapWritable>{

    private Text word = new Text();
    private final static IntWritable row = new IntWritable(0);
    
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), ";");
      
      MapWritable map = new MapWritable();
      
      int col = 1;
      
      while (itr.hasMoreTokens()) {
        map.put(row, new Text(itr.nextToken()));
        context.write(new LongWritable(col),map);
        col ++;
      }
      row.set(row.get()+1);
    }
  }

  public static class IntSumReducer
  extends Reducer<LongWritable, MapWritable, LongWritable, Text> {
                private Text result = new Text();

                public void reduce(LongWritable key, Iterable<MapWritable> maps, Context context
                                  ) throws IOException, InterruptedException {
                	SortedMap<IntWritable,Text> rowVals = new TreeMap<IntWritable,Text>();
                	StringBuffer line = new StringBuffer();
                	
                	for (MapWritable map : maps) {
                		for(Entry<Writable, Writable>  entry : map.entrySet()) {
                            rowVals.put((IntWritable)entry.getKey(),(Text) entry.getValue());
                		}
                	}
                	
                	for(Text rowVal : rowVals.values()) {
          	          line.append(rowVal.toString());
          	          line.append(";");
          	        }
                	
                	context.write(key,new Text(line.toString()));
                }
}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "pivotfinal");
    
    job.setJarByClass(PivotFinal.class);
    
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(MapWritable.class);
    
    job.setMapperClass(TokenizerMapper.class);
    
    job.setReducerClass(IntSumReducer.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}