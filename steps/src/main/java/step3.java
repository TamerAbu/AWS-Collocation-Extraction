import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class step3 {

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
            context.write(new Text(decade + " " + word2 + " " + "*"), new Text(count));// to count c(w2)
            context.write(new Text(decade + " " + word2 + " " + word1), new Text(count));
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
        private Double word2Total = 0.0;
        private String currWord2 = "";

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            double dacadeCount = 0.0;
            double pmi = 0.0;
            String[] parts = key.toString().split(" ");
            String decade = parts[0];
            String word2 = parts[1];
            String word1 = parts[2];
            if (!currWord2.equals(word2)) {// reset for every new word2
                currWord2 = word2;
                word2Total = 0.0;
            }
            for (Text val : values) {
                String[] counters = val.toString().split(" ");
                sum += Double.parseDouble(counters[0]);
                dacadeCount += Double.parseDouble(counters[1]);
                pmi += Double.parseDouble(counters[2]);
            }
            if (word1.equals("*")) {// if word1 is * then it is the total count of word2
                word2Total += sum;
            } else {
                // now pmi is ready and we can calculate nmpi
                double finalPmi = (Math.log((double)sum) - Math.log((double)word2Total) + pmi);
                double npmi= finalPmi/(-Math.log(((double)sum)/(double)dacadeCount));
                context.write(new Text(decade + " " + word1 + " " + word2),new Text(String.valueOf(npmi)));
            }
        }
    }
}
