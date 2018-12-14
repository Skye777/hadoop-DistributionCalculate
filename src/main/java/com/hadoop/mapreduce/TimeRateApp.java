package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 计算出用户在各个时间段通话时长所占比例
 */
public class TimeRateApp {

    /**
     * 读取输入的文件
     */
    public static class TimeRateMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            if (line == null || line.equals("")) {
                return;
            }
            String[] splits = line.split("\t");

            String calling_nbr = splits[1];
            String start_time = splits[9];
            String end_time = splits[10];

            //System.out.println("主叫号码："+calling_nbr +"\t\t"+"开始时间："+start_time+"\t\t"+"结束时间："+end_time);

            outKey.set(calling_nbr);
            outValue.set(new Text(start_time + "\t" + end_time));

            context.write(outKey, outValue);

        }
    }

    /**
     * 归并操作
     */
    public static class TimeRateReducer extends Reducer<Text, Text, Text, Text> {

        private Text name = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int callcount = 0;
            float part[] = {0, 0, 0, 0, 0, 0, 0, 0};
            DecimalFormat df = new DecimalFormat("0.00%");

            for (Text value : values) {

                String line = value.toString();
                if (line == null || line.equals("")) {
                    return;
                }
                String[] splits = line.split("\t");

                String start_time = splits[0];
                String end_time = splits[1];
                SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
                try {
                    Date startime = sdf.parse(start_time);
                    Date endtime = sdf.parse(end_time);

                    int starthour = Integer.parseInt(start_time.substring(0, 2));
                    int endhour = Integer.parseInt(end_time.substring(0, 2));
                    int startmin = Integer.parseInt(start_time.substring(3, 5));
                    int endmin = Integer.parseInt(end_time.substring(3, 5));

                    int smin = starthour * 60 + startmin;
                    int emin = endhour * 60 + endmin;
                    float avgmin = (smin + emin) / 2;
                    //System.out.println(avgmin);

                    if ((avgmin >= 0) && (avgmin < 180)) {
                        part[0]++;
                    } else if ((avgmin >= 180) && (avgmin < 360)) {
                        part[1]++;
                    } else if ((avgmin >= 360) && (avgmin < 540)) {
                        part[2]++;
                    } else if ((avgmin >= 540) && (avgmin < 720)) {
                        part[3]++;
                    } else if ((avgmin >= 720) && (avgmin < 900)) {
                        part[4]++;
                    } else if ((avgmin >= 900) && (avgmin < 1080)) {
                        part[5]++;
                    } else if ((avgmin >= 1080) && (avgmin < 1260)) {
                        part[6]++;
                    } else if ((avgmin >= 1260) && (avgmin < 1440)) {
                        part[7]++;
                    }


                } catch (ParseException e) {
                    e.printStackTrace();
                }
                //System.out.println("开始时间："+start_time+"\t\t"+"结束时间："+end_time);

                callcount++;
            }

            name.set(key);
            //Float[] results = {(float)0};
            for (int i = 0; i <= 7; i++) {
                part[i] = part[i] / (float) callcount;

            }

            context.write(name, new Text(df.format(part[0]) + "\t" + df.format(part[1])
                    + "\t" + df.format(part[2]) + "\t" + df.format(part[3]) + "\t" + df.format(part[4])
                    + "\t" + df.format(part[5]) + "\t" + df.format(part[6]) + "\t" + df.format(part[7]) + "\t"));
        }
    }

    /**
     * 定义Driver，封装TimeRate的所有信息
     */
    public static void main(String[] args) throws Exception{

        // 本地测试
        args = new String[]{"input/tb_call_201202_random.txt", "output/TimeRate"};

        // 创建configuration
        Configuration conf = new Configuration();

        // 清理已存在的目录
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("output file exist, but has been deleted");
        }

        // 创建Job
        Job job = Job.getInstance(conf, "TimeRate");

        // 设置Job处理类
        job.setJarByClass(TimeRateApp.class);

        // 设置map相关参数
        job.setMapperClass(TimeRateMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置reducer相关参数
        job.setReducerClass(TimeRateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setNumReduceTasks(1);

        // 设置作业的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 设置作业的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
