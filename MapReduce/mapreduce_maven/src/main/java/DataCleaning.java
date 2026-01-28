import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class DataCleaning {

    private static long lastOutputCount = 0;

    public static long getLastOutputCount() {
        return lastOutputCount;
    }

    public enum DataCounters {
        TOTAL_INPUT, VALID_GAMES, INVALID_JSON, INVALID_DATA, DUPLICATES, OUTPUT_LINES
    }

    // --- MAPPER ---
    public static class CleaningMapper extends Mapper<Object, Text, Text, Text> {
        private static final Gson gson = new Gson();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(DataCounters.TOTAL_INPUT).increment(1);
            String line = value.toString();
            Game game;

            try {
                game = gson.fromJson(line, Game.class);
            } catch (JsonSyntaxException e) {
                context.getCounter(DataCounters.INVALID_JSON).increment(1);
                return;
            }

            if (game == null || !game.isValid()) {
                context.getCounter(DataCounters.INVALID_DATA).increment(1);
                return;
            }

            String pairKey = game.getPlayerPairKey();
            if (pairKey == null || pairKey.isEmpty()) {
                context.getCounter(DataCounters.INVALID_DATA).increment(1);
                return;
            }

            context.getCounter(DataCounters.VALID_GAMES).increment(1);
            context.write(new Text(pairKey), new Text(line));
        }
    }

    // --- REDUCER ---
    public static class CleaningReducer extends Reducer<Text, Text, Text, NullWritable> {
        private static final Gson gson = new Gson();

        private static class LightGame implements Comparable<LightGame> {
            long timestamp;
            String rawJson;

            public LightGame(long timestamp, String rawJson) {
                this.timestamp = timestamp;
                this.rawJson = rawJson;
            }

            @Override
            public int compareTo(LightGame other) {
                return Long.compare(this.timestamp, other.timestamp);
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<LightGame> buffer = new ArrayList<>();

            for (Text val : values) {
                String json = val.toString();
                try {
                    Game temp = gson.fromJson(json, Game.class);
                    if (temp != null) {
                        buffer.add(new LightGame(temp.getTimestampMillis(), json));
                    }
                } catch (Exception e) {
                    // Ignorer malformés
                }
            }

            if (buffer.isEmpty()) return;

            Collections.sort(buffer);

            LightGame lastValid = buffer.get(0);
            context.write(new Text(lastValid.rawJson), NullWritable.get());
            context.getCounter(DataCounters.OUTPUT_LINES).increment(1);

            for (int i = 1; i < buffer.size(); i++) {
                LightGame current = buffer.get(i);
                
                long timeDiff = Math.abs(current.timestamp - lastValid.timestamp);

                if (timeDiff < 10000) { // 10 secondes
                    context.getCounter(DataCounters.DUPLICATES).increment(1);
                } else {
                    context.write(new Text(current.rawJson), NullWritable.get());
                    context.getCounter(DataCounters.OUTPUT_LINES).increment(1);
                    lastValid = current;
                }
            }
        }
    }

    // --- MAIN ---
    public static boolean runJob(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // conf.set("mapreduce.map.memory.mb", "2048");
        // conf.set("mapreduce.reduce.memory.mb", "4096");

        Job job = Job.getInstance(conf, "PLE Clash Royale - Data Cleaning (Optimized)");

        job.setJarByClass(DataCleaning.class);
        job.setMapperClass(CleaningMapper.class);
        job.setReducerClass(CleaningReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        if (success) {
            Counters c = job.getCounters();
            lastOutputCount = c.findCounter(DataCounters.OUTPUT_LINES).getValue();
            System.out.println("\n-------------------------------------------");
            System.out.println("  RAPPORT DE NETTOYAGE");
            System.out.println("-------------------------------------------");
            System.out.println("  Total lu       : " + c.findCounter(DataCounters.TOTAL_INPUT).getValue());
            System.out.println("  JSON invalides : " + c.findCounter(DataCounters.INVALID_JSON).getValue());
            System.out.println("  Data invalides : " + c.findCounter(DataCounters.INVALID_DATA).getValue());
            System.out.println("  Doublons suppr : " + c.findCounter(DataCounters.DUPLICATES).getValue());
            System.out.println("  Total écrit    : " + c.findCounter(DataCounters.OUTPUT_LINES).getValue());
            System.out.println("-------------------------------------------\n");
        }

        return success;
    }
    
    public static void main(String[] args) throws Exception {
       System.exit(runJob(args) ? 0 : 1);
    }
}