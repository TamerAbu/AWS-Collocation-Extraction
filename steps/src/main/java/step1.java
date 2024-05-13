import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
// import java.util.Random;

public class step1 {
    public static class NgramMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static Pattern ENGLISH_2GRAM_REGEX = Pattern.compile("^[a-zA-Z]+\\s[a-zA-Z]+$");
        private String[] stopWordsArr = { "a", "about", "above", "across", "after", "afterwards", "again", "against",
                "all", "almost", "alone", "along", "already",
                "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "another",
                "any", "anyhow", "anyone", "anything",
                "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became", "because", "become",
                "becomes", "becoming", "been", "before", "beforehand",
                "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but",
                "by", "call", "can", "cannot", "cant", "co",
                "computer", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due",
                "during", "each", "eg", "eight", "either",
                "eleven", "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone",
                "everything", "everywhere", "except", "few", "fifteen",
                "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four",
                "from", "front", "full", "further", "get", "give",
                "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein",
                "hereupon", "hers", "herself", "him", "himself", "his",
                "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it",
                "its", "itself", "keep", "last", "latter", "latterly",
                "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more",
                "moreover", "most", "mostly", "move", "much", "must",
                "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody",
                "none", "noone", "nor", "not", "nothing", "now",
                "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others",
                "otherwise", "our", "ours", "ourselves", "out", "over",
                "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed",
                "seeming", "seems", "serious", "several", "she",
                "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone",
                "something", "sometime", "sometimes", "somewhere",
                "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then",
                "thence", "there", "thereafter", "thereby",
                "therefore", "therein", "thereupon", "these", "they", "thick", "thin", "third", "this", "those",
                "though", "three", "through", "throughout", "thru",
                "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under",
                "until", "up", "upon", "us", "very", "via", "was",
                "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter",
                "whereas", "whereby", "wherein", "whereupon", "wherever",
                "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will",
                "with", "within", "without", "would", "yet", "you", "your",
                "yours", "yourself", "yourselves" };

        private Set<String> stopWords = new HashSet<String>(Arrays.asList(stopWordsArr)); // keeping as Set for faster lookup
        // private Random random = new Random();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // if (random.nextDouble() <= 0.05) {// works on 5 precent of the input
                String[] split = value.toString().split("\t");
                if (split.length < 5) {
                    return;
                }
                String nGram = split[0];
                int year = Integer.parseInt(split[1]);
                String matchCount = split[2];
                int decade = (year / 10) * 10;
                String[] words = nGram.split(" ");
                StringTokenizer count = new StringTokenizer(nGram, " ");
                if (count.countTokens() == 2 && ENGLISH_2GRAM_REGEX.matcher(nGram).matches()) { 
                    if (stopWords.contains(words[0].toLowerCase()) || stopWords.contains(words[1].toLowerCase()))
                        return;
                        context.write(new Text(decade + " " + "*"+" "+"N"), new Text(matchCount));  
                        context.write(new Text(decade + " " + nGram), new Text(matchCount));   
                }
            // }
        }
    }

    public static class NgramReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for (Text val : values) {
                double count= Double.parseDouble(val.toString());
                sum += count;
            }
            context.write(key, new Text(String.valueOf(sum)));
        }
    }
}
