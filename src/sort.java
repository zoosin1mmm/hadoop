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
        
public class sort {

 public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            //auto sort key during shuffle,so set value in key 
            context.write(new IntWritable(Integer.parseInt(token)),new IntWritable(1));
        }
    }
 } 
        
 public static class Reduce extends Reducer<IntWritable, IntWritable, Text, IntWritable> {

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        //long sum = 0;
        //int count=0;
        for (IntWritable val : values) {
           // sum += val.get();
           // count++;
           context.write(new Text(""), key);
        }
        //sum = sum/count;
        //context.write(key, new LongWritable(sum));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "sum");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapOutputKeyClass(IntWritable.class);
    //job.setMapOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(sort.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
