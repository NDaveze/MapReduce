import java.io.IOException;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PivotFinal {

  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, LongWritable, MapWritable>{

    private Text word = new Text();
    private final static IntWritable row = new IntWritable(1);
    
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), ";");
      
      MapWritable map = new MapWritable();
      
      int col = 1;
      
      while (itr.hasMoreTokens()) {
        map.put(new Text(row.toString()), new Text(itr.nextToken()));
        context.write(new LongWritable(col),map);
        col ++;
      }
      row.set(1);
    }
  }

  public static class IntSumReducer
  extends Reducer<LongWritable, MapWritable, LongWritable, Text> {
                private Text result = new Text();

                public void reduce(LongWritable key, Iterable<MapWritable> maps, Context context
                                  ) throws IOException, InterruptedException {
                	SortedMap<LongWritable,IntWritable> rowVals = new TreeMap<LongWritable,IntWritable>();
                	String cell = new String();

                	for (MapWritable map : maps) {
                		for(Entry<Writable, Writable>  entry : map.entrySet()) {
                            rowVals.put((LongWritable) entry.getKey(),(IntWritable) entry.getValue());
                }
                            cell += new String(val.toString());
                            cell += ";";
                    }



                        result.set(new Text(cell.toString()));
                        context.write(key, result);

                }
}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "pivotfinal");
    job.setJarByClass(Pivot.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}