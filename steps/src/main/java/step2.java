import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class step2 {

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
            if (word1.equals("*") && word2.equals("N")) {// for line: total count N per dacade
                context.write(new Text(decade + " " + word1 + " " + word2), new Text(count));
            } else {
                context.write(new Text(decade + " " + word1 + " " + "*"), new Text(count));// to count c(w1)
                context.write(new Text(decade + " " + word1 + " " + word2), new Text(count));
            }
        }
    }

    public static class YearPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] parts = key.toString().split(" ");
            String decade = parts[0];
            // Use a hash of the `year word1` part of the key to partition
            return ((decade.hashCode() & 0xFFFFFFF) % numPartitions);
        }
    }

    public static class NGramReducer extends Reducer<Text, Text, Text, Text> {
        private double word1Total = 0.0;
        private String currWord1 = "";
        private double dacadeCount = 0.0;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            String[] parts = key.toString().split(" ");
            String word1 = parts[1];
            String word2 = parts[2];
            if (word1.equals("*") && word2.equals("N")) { // every time we get new dacede count line its have to be new dacade
                for (Text val : values) {
                    dacadeCount = Double.parseDouble(val.toString());
                }
            } else {
                if (!currWord1.equals(word1)) {
                    currWord1 = word1;
                    word1Total = 0.0;
                }
                for (Text val : values) {
                    sum += Double.parseDouble(val.toString());
                }
                if (word2.equals("*")) {
                    word1Total += sum;
                } else {
                    // pmi so far = log(N)-log(c(w1))
                    context.write(key, new Text(String.valueOf(sum + " " + dacadeCount + " "
                            + (Math.log((double) dacadeCount) - Math.log((double) word1Total)))));
                }
            }
        }
    }
}
