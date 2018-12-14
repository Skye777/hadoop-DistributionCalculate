package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * 计算出用户的每日平均通话次数
 */
public class AvgCallApp {

    /**
     * 读取输入的文件
     */
    public static class AvgCallMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            if (line == null || line.equals("")) {
                return;
            }

            String[] splits = line.split("\t");

            String day_id = splits[0];
            String calling_nbr = splits[1];
            //System.out.println(calling_nbr +"\t\t"+day_id );

            outKey.set(calling_nbr);
            outValue.set(day_id);

            context.write(outKey, outValue);

        }
    }

    /**
     * 归并结果
     */
    public static class AvgCallReducer extends Reducer<Text, Text, Text, FloatWritable> {

        private FloatWritable avg = new FloatWritable();
        private Text name = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //System.out.println(key +"\t\t"+value);
            float average = 0;
            float daycount = 0;
            float callcount = 0;

            String temp = "";

            for (Text value : values) {

                //System.out.println("val: "+val.toString()+" temp: "+temp);
                String tempVal = value.toString();

                if (!temp.equals(tempVal)) {
                    daycount += 1;
                    temp = tempVal;
                }

                callcount++;
            }

            average = callcount / daycount;

            name.set(key);
            avg.set(average);
            context.write(name, avg);
        }
    }

    /**
     * 定义Diver，封装AvgCall的所有信息
     */
    public static void main(String[] args) throws Exception{

        // 本地测试
        args = new String[]{"input/tb_call_201202_random.txt","output/AvgCall"};

        // 创建configuration
        Configuration conf = new Configuration();

        // 清理已存在的目录
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath,true);
            System.out.println("output file exist, but has been deleted");
        }

        // 创建Job
        Job job = Job.getInstance(conf, "AvgCall");

        // 设置Job处理类
        job.setJarByClass(AvgCallApp.class);

        // 设置map相关参数
        job.setMapperClass(AvgCallMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置reducer相关参数
        job.setReducerClass(AvgCallReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
//        job.setNumReduceTasks(1);

        // 设置作业的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 设置作业的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


}
