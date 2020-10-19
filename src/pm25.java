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
 public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        String point_str ="";
        String rain_str ="";
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if(point_str=="") point_str=token.trim();
            else rain_str = token.trim();
        }
        // 表示為第一次讀檔案
        if(rain_str==""){
            rain_str=point_str;
            point_str = "";
        }

        String[] tokens = rain_str.split(",");
        int index=0;
        // 先取四點 當中心點
        if(!full)
        {
            // 表示為第一次讀檔案
            String[][] point_tokens = {{},{},{},{}};
            if(point_str!="") 
            {
                String[] point_tmp = {};
                point_tmp=point_str.split(";");
                for (int i=0;i<point_tmp.length-1 ;i++ ) {
                    point_tokens[i]= point_tmp[i].split(",");
                }
            }
            for (index=0; index<4;index++ ) 
            {
                if(point[index].length==0)
                {
                    if(point_str!="") tokens = point_tokens[index];
                    point[index] = new int[tokens.length];
                    for (int i=1;i<tokens.length;i++){
                        if(tokens[i].length()==0) continue;
                        point[index][i-1] = Integer.parseInt(tokens[i]);
                    }
                    if(point_str=="") break;
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
        context.write(new IntWritable(index), new Text(rain_str));
    } 
}  


 public static class Reduce extends Reducer<IntWritable,Text , IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        String[] token={};
        int[] rain={};
        boolean repeat=false;
        int count = 0;
        int point_k = key.get();
        // 字串切割
        for (Text val : values) {
            token = val.toString().trim().split(",");
            if(rain.length==0)
                rain = new int[token.length-1];
            // 雨量平均
            boolean tmp_repeat=true;
            for (int i=1; i<token.length; i++) {
                if(token[i].length()==0) continue;
                rain[i-1]+=Integer.parseInt(token[i]);
                // 群中心是否有重複
                if(point[point_k][i-1]!=Integer.parseInt(token[i])) tmp_repeat = false;
            }
            if(tmp_repeat) repeat=true;
            count++;
            context.write(key, val);
        }
        // 無重複要加入計算平均
        if(!repeat)
        {
            for (int i=0; i<rain.length; i++) {
                rain[i]+=point[point_k][i];
            }
            count++;
        }
        // // 更新群中心
        String output = "";
        for (int i=0; i<rain.length; i++) {
            point[point_k][i]=rain[i]/count;
            output+=String.valueOf(point[point_k][i])+",";
        }
        context.write(new IntWritable(10+point_k), new Text(output));
    }

 }

 public static class SortMap2 extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        String tag ="";
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            tag=token+";";
            token = tokenizer.nextToken();
            context.write(new Text("1"), new Text(tag+token));
        }
    }
}
 public static class SortReduce2 extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
 	String[] token={};
        String[][] tokens={};
        String result_k = "";
        int count=0;

        for (Text val : values) {
            token = val.toString().trim().split(";");
            if(Integer.parseInt(token[0].trim())>=10)
                result_k+="t,"+token[1]+";";
        }
	for (Text val : values) {
            token = val.toString().trim().split(";");
            if(Integer.parseInt(token[0].trim())<10)
		context.write(new Text(result_k+token[0]), new Text(token[1]));
        }
}
}

 public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    String intput_path=args[0];
    String output_path = args[1];
    String orign_path= args[1];
    // KMEAN
   int i=0;
   for (i=0;i<1;i++ ) {
    // 計算
        Job job = new Job(conf, "pm25_calcu_"+i);
        
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
        output_path = orign_path+"c_"+String.valueOf(i);
    // 排序
        Job job2 = new Job(conf, "pm25_sort_"+i);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
            
        job2.setMapperClass(SortMap2.class);
        job2.setReducerClass(SortReduce2.class);
        job2.setJarByClass(pm25.class);
            
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(intput_path));
        FileOutputFormat.setOutputPath(job2, new Path(output_path));
        job2.waitForCompletion(true);
        intput_path = output_path;
        output_path = orign_path+"s_"+String.valueOf(i);
    }
  }
}

