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
 static float[][] point={{},{},{},{}};
 static boolean full = false;
 //第一次 日期在第一位 第二次後 日期在第二位
 public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().trim().split(",");
        int index=0;
        // 先取四點 當中心點
        if(!full)
        {
            for (index=0; index<4;index++ ) 
            {
                if(point[index].length==0)
                {
                    point[index] = new float[tokens.length];
                    for (int i=1;i<tokens.length;i++){
                        if(tokens[i].length()==0) continue;
                        point[index][i-1] = (float)Integer.parseInt(tokens[i]);
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
            for (int i=1;i<tokens.length;i++){
                if(tokens[i].length()==0) continue;
                tmp = Integer.parseInt(tokens[i]);
                for (int j =0 ; j<4 ;j++ ) {
                    distance[j]+= (tmp - point[j][i-1])*(tmp - point[j][i-1]);
                 }
            }
            // 最小
            long min =0;
            for (int i =0 ; i<4 ;i++ ) {
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
        float[] rain={};
        boolean repeat=false;
        int count = 0;
        int point_k = key.get();
        // 字串切割
        for (Text val : values) {
            token = val.toString().trim().split(",");
            if(rain.length==0)
                rain = new float[token.length-1];
            // 雨量平均
            boolean tmp_repeat=true;
            for (int i=1; i<token.length; i++) {
                if(token[i].length()==0) continue;
                rain[i-1]+=(float)Integer.parseInt(token[i]);
                // 群中心是否有重複
                if(point[point_k][i-1]!=(float)Integer.parseInt(token[i])) tmp_repeat = false;
            }
            if(tmp_repeat) repeat=true;
            context.write(key, val);
            count++;
        }
        // 無重複要加入計算平均
        if(!repeat)
        {
            for (int i=0; i<rain.length; i++) {
                rain[i]+=point[point_k][i];
            }
            context.write(new IntWritable(10), new Text("add"));
            count++;
        }
        // // 更新群中心
        String output = "";
        String output2 = "";
        String output3 = "";
        for (int i=0; i<rain.length; i++) {
            output3+=String.valueOf(point[point_k][i])+",";
            point[point_k][i]=rain[i]/count;
            output2+=String.valueOf(rain[i])+",";
            output+=String.valueOf(point[point_k][i])+",";
        }
        context.write(new IntWritable(10), new Text(output));
        context.write(new IntWritable(count), new Text(output2));
        context.write(new IntWritable(10), new Text(output3));
    }

 }

 public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    String intput_path=args[0];
    String output_path = args[1];
    String orign_path= args[1];
    // KMEAN
   int i=0;
   for (i=0;i<2;i++ ) {
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
        output_path = orign_path+"_"+String.valueOf(i);
    }
  }
}
