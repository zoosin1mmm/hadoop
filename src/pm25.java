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
        
public class pm25 {
 static int[][] point={{},{},{},{}};
 static boolean full = false;
 //第一次 日期在第一位 第二次後 日期在第二位
 static int date_index=0;
 public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        int index=0;
        // 先取四點 當中心點
        if(!full)
        {
            for (index=0; index<4;index++ ) 
            {
                if(point[index].length==0)
                {
                    point[index] = new int[tokens.length];
                    for (int i=date_index+1;i<tokens.length;i++){
                        if(tokens[i].length==0) continue;
                        point[index][i-date_index-1] = Integer.parseInt(tokens[i].trim());
                    }
                    break;
                }
            }
            if(index==3) full=true;
        }
        else
        {
            // 計算距離
            long[] distance = {0,0,0,0};//距離
            int tmp = 0;
            for (int i=date_index+1;i<tokens.length;i++){
                if(tokens[i].length==0) continue;
                tmp = Integer.parseInt(tokens[i].trim());
                for (int j =0 ; j<4 ;j++ ) {
                    distance[j]+= (tmp - point[j][i-date_index-1])*(tmp - point[j][i-date_index-1]);
                 }
            }
            // 最小
            long min =0;
            for (int i =1 ; i<4 ;i++ ) {
                // 比較小或第一回 且不是同一個日期 更新min
                if((distance[i]<min||min==0)&&distance[i]!=0)
                {
                    min=distance[i];
                    tmp = i;
                } 
            }
            index = tmp;
        }
        // 分群以數字0-3
        context.write(new IntWritable(index), value);
    } 
}  
 public static class Reduce extends Reducer<IntWritable,Text , IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        String[] token={};
        long[] rain={};
        int count = 0;
        // 字串切割
        for (Text val : values) {
            token = val.toString().split(",");
            if(rain.length==0)
                rain = new long[token.length];
            // 雨量平均
            for (int i=date_index+1; i<token.length; i++) {
                rain[i]+=Integer.parseInt(token[i].trim());
            }
            context.write(key, val);
            count++;
        }
        for (int i=date_index+1; i<token.length; i++) {
            point[key.get()][i-date_index-1]=(int)(rain[i]/count);
        }
    }
    public void cleanup(Context context) throws IOException, InterruptedException {
       date_index=1;
   }
 }

 public static class SortMap extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            context.write(new Text(token),new Text(""));
        }
    }
 } 
        
 public static class SortReduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        for (Text val : values) {
           context.write(key,new Text(""));
        }
    }
}
        
 public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    String intput_path=args[0];
    String output_path = "";
    // KMEAN
   int i=0;
   for (i=0;i<30 ;i++ ) {
        output_path = intput_path+String.valueOf(i);
        Job job = new Job(conf, "pm25");
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
            
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setJarByClass(pm25.class);
            
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(intput_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));
        job.waitForCompletion(true);
        intput_path = output_path;
    }
    // 排序
    output_path = intput_path+String.valueOf(i);
    Job job2 = new Job(conf, "avg");
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
        
    job2.setMapperClass(SortMap.class);
    job2.setReducerClass(SortReduce.class);
    job2.setJarByClass(pm25.class);
        
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job2, new Path(intput_path));
    FileOutputFormat.setOutputPath(job2, new Path(output_path));
        
    job2.waitForCompletion(true);
 }
 }

