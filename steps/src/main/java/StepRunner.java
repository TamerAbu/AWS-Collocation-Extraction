import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class StepRunner {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: java -jar HadoopRunner-1.jar <arg1> <arg2>");
            System.exit(1);
        }

        Configuration conf1 = new Configuration();
        // conf1.set("mapred.max.split.size", String.valueOf(26360131647L/2099)); 

        Job job1 = Job.getInstance(conf1, "Dacade 2Gram Count");

        job1.setJarByClass(step1.class);
        job1.setMapperClass(step1.NgramMapper.class);
        job1.setReducerClass(step1.NgramReducer.class);

        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        SequenceFileInputFormat.addInputPath(job1, new Path(args[0]));

        String job1Output = "s3://YOURBUCKETNAME/output1/";
        FileOutputFormat.setOutputPath(job1, new Path(job1Output));

        // Wait for job1 to complete before proceeding
        boolean job1Completed = job1.waitForCompletion(true);
        if (!job1Completed) {
            System.exit(1); // Exit if job1 fails
        }

        Configuration conf2 = new Configuration();
        // conf2.set("mapred.max.split.size", String.valueOf(9588308162L/459)); 
        Job job2 = Job.getInstance(conf2, "c(w1) calculator");

        job2.setJarByClass(step2.class);
        job2.setMapperClass(step2.NGramMapper.class);
        job2.setPartitionerClass(step2.YearPartitioner.class);
        job2.setReducerClass(step2.NGramReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);

        TextInputFormat.addInputPath(job2, new Path(job1Output));

        String job2Output = "s3://YOURBUCKETNAME/output2/";
        FileOutputFormat.setOutputPath(job2, new Path(job2Output));

        boolean job2Completed = job2.waitForCompletion(true);
        if (!job2Completed) {
            System.exit(1);
        }

        Configuration conf3 = new Configuration();
        // conf3.set("mapred.max.split.size", String.valueOf(22072698035L/937)); 
        Job job3 = Job.getInstance(conf3, "c(w1) calculator");

        job3.setJarByClass(step3.class);
        job3.setMapperClass(step3.NGramMapper.class);
        job3.setPartitionerClass(step3.YearPartitioner.class);
        job3.setReducerClass(step3.NGramReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setInputFormatClass(TextInputFormat.class);

        TextInputFormat.addInputPath(job3, new Path(job2Output));

        String job3Output = "s3://YOURBUCKETNAME/output3/";
        FileOutputFormat.setOutputPath(job3, new Path(job3Output));

        boolean job3Completed = job3.waitForCompletion(true);
        if (!job3Completed) {
            System.exit(1);
        }

        Configuration conf4 = new Configuration();
        // conf4.set("mapred.max.split.size", String.valueOf(20736152353L/704)); 
    
        Job job4 = Job.getInstance(conf4, "npmi pmi calcaulator");

        job4.setJarByClass(step4.class);
        job4.setMapperClass(step4.NGramMapper.class);
        job4.setPartitionerClass(step4.YearPartitioner.class);
        job4.setReducerClass(step4.NgramReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        job4.setInputFormatClass(TextInputFormat.class);

        TextInputFormat.addInputPath(job4, new Path(job3Output));

        String job4Output = "s3://YOURBUCKETNAME/output4/";
        FileOutputFormat.setOutputPath(job4, new Path(job4Output));

        boolean job4Completed = job4.waitForCompletion(true);
        if (!job4Completed) {
            System.exit(1);
        }

        Configuration conf5 = new Configuration();
        conf5.set("mapred.max.split.size", String.valueOf(13883475782L/704));
        conf5.set("minPmi", args[1]); 
        conf5.set("relMinPmi", args[2]);
        Job job5 = Job.getInstance(conf5, "npmi calculator");

        job5.setJarByClass(step5.class);
        job5.setMapperClass(step5.NGramMapper.class);
        job5.setPartitionerClass(step5.YearPartitioner.class);
        job5.setReducerClass(step5.NgramReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);
        job5.setInputFormatClass(TextInputFormat.class);

        TextInputFormat.addInputPath(job5, new Path(job4Output));
        String job5Output = "s3://YOURBUCKETNAME/output5/";
        FileOutputFormat.setOutputPath(job5, new Path(job5Output));
        boolean job5Completed = job5.waitForCompletion(true);
        if (!job5Completed) {
            System.exit(1);
        }


    }
}
