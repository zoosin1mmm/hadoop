import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class avg {

 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            //context.write(new Text("test"), new IntWritable(Integer.parseInt(tokenizer.nextToken())));
            context.write(new Text("test"), new IntWritable(Integer.parseInt(token)));
        }
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, LongWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        long sum = 0;
        int count=0;
        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }
        sum = sum/count;
        context.write(key, new LongWritable(sum));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "sum");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //job.setMapOutputKeyClass(Text.class);
    //job.setMapOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(avg.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
