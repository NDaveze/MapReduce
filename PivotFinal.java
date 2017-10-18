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
    	
    	/*L'input reader envoie les lignes 1 par 1, on les split avec le séparateur ";"*/
    	StringTokenizer itr = new StringTokenizer(value.toString(), ";");
      
    	/*On créé une map (k,v) pour stocker le numero de ligne + valeur de la case lue*/
    	MapWritable map = new MapWritable();
    	
    	
    	int col = 1;
	    /*On parcourt les valeurs de la ligne en cours*/
    	while (itr.hasMoreTokens()) {
    		/*On insert dans map num de ligne + valeur de chaque case*/
	        map.put(row, new Text(itr.nextToken()));
	        /*On transmet au reducer la colonne de chaque case*/
	        context.write(new LongWritable(col),map);
	        col ++;
	    }
    	/*On change de ligne*/
	    row.set(row.get()+1);
    }
  }
  
  public static class IntSumReducer
  extends Reducer<LongWritable, MapWritable, LongWritable, Text> {
                private Text result = new Text();

                public void reduce(LongWritable key, Iterable<MapWritable> maps, Context context
                                  ) throws IOException, InterruptedException {
                	/*On créé une liste de map pour lire maps*/
                	SortedMap<IntWritable,Text> rowVals = new TreeMap<IntWritable,Text>();
                	StringBuffer line = new StringBuffer();
                	
                	/*On parcourt les couples k,v dans maps*/
                	for (MapWritable map : maps) {
                		for(Entry<Writable, Writable>  entry : map.entrySet()) {
                			/*On écrit dans rowvals les valeurs des cases de chaque colonne*/
                            rowVals.put((IntWritable)entry.getKey(),(Text) entry.getValue());
                		}
                	}
                	
                	/*On recopie ces valeurs dans une string, qu'on envoie en output*/
                	for(Text rowVal : rowVals.values()) {
          	          line.append(rowVal.toString());
          	          line.append(";");
          	        }
                	line.delete((line.length() - 1), line.length());
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
    /*l'output est de la forme (colonne,(ligne,valeur))*/
    /*Pas de combiner, car le mapper ne renvoie que des clés uniques*/
    /*Le shuffle and sort renvoie */
    job.setReducerClass(IntSumReducer.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}