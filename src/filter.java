import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class filter {
         
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String[] tokens = value.toString().split(",");
        String result = "";
        Pattern pattern = Pattern.compile("\\d+");
        Matcher matcher;
        if(tokens[1].trim().equals("§å¤å¤")&&tokens[2].trim().equals("PM2.5"))
        {
            result = tokens[0]+","+tokens[1]+",";
            for (int i =0;i<tokens.length ;i++ ) {
                if(i>=3)
                {
                    matcher = pattern.matcher(tokens[i]);
                    while (matcher.find()) {  
                        tokens[i]=matcher.group(0).toString().trim().equals("")?"0":matcher.group(0).toString();
                    }
                    result +=tokens[i].trim()+",";
                }
            }
            context.write(new Text(""), new Text(result));
        }
    }
}
        
 public static class Reduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        for (Text val : values) {
            context.write(new Text(""), new Text(val));
        }
    }
 }

        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "filter");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(filter.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //It have a job after this job is finish        
    job.waitForCompletion(true);
 }
        
}

