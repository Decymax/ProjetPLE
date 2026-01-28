import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Stats {

    // --- JOB 1 : JOINTURE SOURCE ---
    public static class Job1_JoinSource {
        public static class NodeMapper extends Mapper<Object, Text, Text, Text> {
            @Override
            protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString().replaceAll("\t", ";");
                String[] parts = line.split(";");
                if (parts.length >= 2) {
                    context.write(new Text(parts[0].trim()), new Text("NODE;" + parts[1].trim()));
                }
            }
        }
        public static class EdgeMapper extends Mapper<Object, Text, Text, Text> {
            @Override
            protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString().replaceAll("\t", ";");
                String[] parts = line.split(";");
                if (parts.length >= 4) {
                    context.write(new Text(parts[0].trim()), new Text("EDGE;" + parts[1].trim() + ";" + parts[2].trim() + ";" + parts[3].trim()));
                }
            }
        }
        public static class JoinReducer extends Reducer<Text, Text, Text, NullWritable> {
            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                String countSource = null;
                List<String> edges = new ArrayList<>();
                for (Text val : values) {
                    String s = val.toString();
                    if (s.startsWith("NODE;")) countSource = s.substring(5);
                    else if (s.startsWith("EDGE;")) edges.add(s.substring(5));
                }
                if (countSource != null) {
                    for (String edge : edges) {
                        context.write(new Text(key.toString() + ";" + edge + ";" + countSource), NullWritable.get());
                    }
                }
            }
        }
    }

    // --- JOB 2 : JOINTURE TARGET ---
    public static class Job2_JoinTarget {
        public static class NodeMapper extends Mapper<Object, Text, Text, Text> {
            @Override
            protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString().replaceAll("\t", ";");
                String[] parts = line.split(";");
                if (parts.length >= 2) {
                    context.write(new Text(parts[0].trim()), new Text("NODE;" + parts[1].trim()));
                }
            }
        }
        public static class EdgeMapper extends Mapper<Object, Text, Text, Text> {
            @Override
            protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString(); 
                String[] parts = line.split(";");
                if (parts.length >= 5) {
                    context.write(new Text(parts[1].trim()), new Text("EDGE;" + line));
                }
            }
        }

        public static class JoinReducer extends Reducer<Text, Text, Text, NullWritable> {
            long nAll = 1;
            int k = 8; 

            @Override
            protected void setup(Context context) {
                nAll = context.getConfiguration().getLong("nAll", 1);
                // Récupération de la taille, par défaut 8
                k = context.getConfiguration().getInt("size", 8);
            }

            // Calcul coefficient binomial
            private long nCr(int n, int r) {
                if (r > n) return 0;
                if (r == 0 || r == n) return 1;
                long res = 1;
                for (int i = 1; i <= r; i++) res = res * (n - i + 1) / i;
                return res;
            }

            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                long countTarget = 0;
                List<String> edges = new ArrayList<>();

                for (Text val : values) {
                    String s = val.toString();
                    if (s.startsWith("NODE;")) {
                        try { countTarget = Long.parseLong(s.substring(5)); } catch(Exception e) {}
                    } else if (s.startsWith("EDGE;")) {
                        edges.add(s.substring(5));
                    }
                }

                if (countTarget > 0) {
                    long combinations = nCr(8, k); 
                    
                    double correctionFactor = (double) combinations * combinations;

                    for (String edge : edges) {
                        String[] parts = edge.split(";");
                        if (parts.length < 5) continue;

                        String source = parts[0];
                        String target = parts[1];
                        long count = Long.parseLong(parts[2]);
                        long win = Long.parseLong(parts[3]);
                        long countSource = Long.parseLong(parts[4]);

                        double rawPrevision = (double) (countSource * countTarget) / nAll;
                        
                        // Application du facteur
                        double finalPrevision = rawPrevision * correctionFactor;

                        String out = source + ";" + target + ";" + count + ";" + win + ";" + 
                                     countSource + ";" + countTarget + ";" + 
                                     String.format("%.2f", finalPrevision).replace(',', '.');
                        
                        context.write(new Text(out), NullWritable.get());
                    }
                }
            }
        }
    }

    // --- MAIN RUNNER ---
    public static boolean runJob(String nodesPath, String edgesPath, String outputPath, long nAll, int archetypeSize) throws Exception {
        Configuration conf = new Configuration();
        Path outPath = new Path(outputPath);
        String safeOutputName = outPath.getName();
        Path tempPath = new Path(outPath.getParent(), safeOutputName + "_temp_job1");

        // Job 1
        Job job1 = Job.getInstance(conf, "Stats - Join Source");
        job1.setJarByClass(Stats.class);
        MultipleInputs.addInputPath(job1, new Path(nodesPath), TextInputFormat.class, Job1_JoinSource.NodeMapper.class);
        MultipleInputs.addInputPath(job1, new Path(edgesPath), TextInputFormat.class, Job1_JoinSource.EdgeMapper.class);
        job1.setReducerClass(Job1_JoinSource.JoinReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(tempPath)) fs.delete(tempPath, true);
        FileOutputFormat.setOutputPath(job1, tempPath);
        if (!job1.waitForCompletion(true)) return false;
        
        // Job 2
        Configuration conf2 = new Configuration();
        conf2.setLong("nAll", nAll);
        conf2.setInt("size", archetypeSize); // Passage du paramètre
        
        Job job2 = Job.getInstance(conf2, "Stats - Join Target (k=" + archetypeSize + ")");
        job2.setJarByClass(Stats.class);
        MultipleInputs.addInputPath(job2, new Path(nodesPath), TextInputFormat.class, Job2_JoinTarget.NodeMapper.class);
        MultipleInputs.addInputPath(job2, tempPath, TextInputFormat.class, Job2_JoinTarget.EdgeMapper.class);
        job2.setReducerClass(Job2_JoinTarget.JoinReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        
        if (fs.exists(outPath)) fs.delete(outPath, true);
        FileOutputFormat.setOutputPath(job2, outPath);
        
        boolean success = job2.waitForCompletion(true);
        if (success) fs.delete(tempPath, true);
        
        return success;
    }
}