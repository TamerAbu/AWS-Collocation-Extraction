import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class step4 {
    public static class NGramMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().split("\t");
            if (keyValue.length < 2) {
                return;
            }
            String[] parts = keyValue[0].split(" ");
            String decade = parts[0];
            String word1 = parts[1];
            String word2 = parts[2];
            String count = keyValue[1];
            context.write(new Text(decade + " " + "*" + " " + "npmi"), new Text(count));
            context.write(new Text(decade + " " + word1 + " " + word2), new Text(count));
        }
    }   

    public static class YearPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] parts = key.toString().split(" ");
            String decade = parts[0];
            // Use a hash of the `year` part of the key to partition
            return ((decade.hashCode() & 0xFFFFFFF) % numPartitions);
        }
    }

    public static class NgramReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Double sum = 0.0;
            String[] parts = key.toString().split(" ");
            String word1 = parts[1];
            String word2 = parts[2];
            if(word1.equals("*")&&word2.equals("npmi")){
                for (Text val : values) {
                    Double count=Double.parseDouble(val.toString());
                    sum += count;
                }
                context.write(key, new Text(String.valueOf(sum)));
            } else{
                for (Text val : values) {
                    context.write(key, val);
                }
            }
        }
    }
    
}
