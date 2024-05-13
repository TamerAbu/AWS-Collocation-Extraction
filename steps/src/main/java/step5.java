import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class step5 {
    public static class NGramMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().split("\t");
            String[] parts = keyValue[0].split(" ");
            String decade = parts[0];
            String word1 = parts[1];
            String word2 = parts[2];
            String count = keyValue[1];
            if (word1.equals("*") && word2.equals("npmi")) {
                context.write(new Text(decade + " " + "*"), new Text(count));
            } else {
                context.write(new Text(decade + " " + count), new Text(word1 + " " + word2));
            }
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

        private String relMinPmi;
        private String minPmi;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            relMinPmi = conf.get("relMinPmi");
            minPmi = conf.get("minPmi");
        }

        private Double npmiCount = 0.0;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] parts = key.toString().split(" ");
            String decade = parts[0];
            String count = parts[1];
            if (count.equals("*")) { // evrey time we get new npmi
                // count line its have to be new
                for (Text val : values) {
                    npmiCount = Double.parseDouble(val.toString());
                }
            } else {
                for (Text val : values) {
                    String[] words = val.toString().split(" ");
                    String word1 = words[0];
                    String word2 = words[1];
                    Double npmi = Double.parseDouble(count);
                    if (npmi < 1) {
                        if (npmi >= Double.parseDouble(minPmi) || (npmi / npmiCount) >= Double.parseDouble(relMinPmi)) {
                            context.write(new Text(decade + " " + word1 + " " + word2), new Text(String.valueOf(npmi)));
                        }
                    }
                }
            }

        }
    }
}
