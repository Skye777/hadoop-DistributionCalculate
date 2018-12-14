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

/**
 * 计算出不同通话类型(市话、长途、国际)下
 * 各个运营商(移动，联通，电信)的占比
 */
public class TypeRateApp {

    /**
     * 读取输入的文件
     */
    public static class TypeRateMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            if (line == null || line.equals("")) {
                return;
            }
            String[] splits = line.split("\t");

            String calling_type = splits[12];
            String calling_optr = splits[4];

            //System.out.println("通话类型为："+calling_type +"\t\t"+ "运营商为："+calling_optr );

            outKey.set(calling_type);
            outValue.set(calling_optr);

            context.write(outKey, outValue);
        }
    }

    /**
     * 归并结果
     */
    public static class TypeRateReducer extends Reducer<Text, Text, Text, Text> {

        private Text avg = new Text();
        private Text name = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int callcount = 0;
            int calltype1 = 0; //电信
            int calltype2 = 0; //移动
            int calltype3 = 0; //联通
            float percentage1 = 0;
            float percentage2 = 0;
            float percentage3 = 0;
            DecimalFormat df = new DecimalFormat("0.00%");

            for (Text val : values) {

                String tempval = val.toString();
                int temp = Integer.parseInt(tempval);
                if (temp == 1) {
                    calltype1++;
                }
                if (temp == 2) {
                    calltype2++;
                }
                if (temp == 3) {
                    calltype3++;
                }
                callcount++;
            }
            percentage1 = (float) calltype1 / (float) callcount;
            percentage2 = (float) calltype2 / (float) callcount;
            percentage3 = (float) calltype3 / (float) callcount;
            System.out.println(calltype1 + "  " + calltype2 + "  " + calltype3 + " " + callcount);
            name.set(key);
            Float[] results = {percentage1, percentage2, percentage3};
            //avg.set(Float.toString((percentage1)));
            //avg.set(percentage2);
            //avg.set(percentage3);
            context.write(name, new Text(df.format(percentage1) + "\t" + df.format(percentage2) + "\t" + df.format(percentage3)));
        }
    }

    /**
     * 定义Driver，封装TypeRate的所有信息
     */
    public static void main(String[] args) throws Exception {

        // 本地测试
        args = new String[]{"input/tb_call_201202_random.txt", "output/TypeRate"};

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
        Job job = Job.getInstance(conf, "TypeRate");

        // 设置Job处理类
        job.setJarByClass(TypeRateApp.class);

        // 设置map相关参数
        job.setMapperClass(TypeRateMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置reducer相关参数
        job.setReducerClass(TypeRateReducer.class);
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
