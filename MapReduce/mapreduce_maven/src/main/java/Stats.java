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
                // reçoit : Archetype;Count;Win
                String line = value.toString().replaceAll("\t", ";");
                String[] parts = line.split(";");

                if (parts.length < 2) return; 

                String archetype = parts[0].trim();
                String count = parts[1].trim();
                
                // Clé : Archetype | Valeur : NODE;Count
                context.write(new Text(archetype), new Text("NODE;" + count));
            }
        }

        public static class EdgeMapper extends Mapper<Object, Text, Text, Text> {
            @Override
            protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                // reçoit : Source;Target;Count;Win
                String line = value.toString().replaceAll("\t", ";");
                String[] parts = line.split(";");

                if (parts.length < 4) return;

                String source = parts[0].trim();
                String target = parts[1].trim();
                String count = parts[2].trim();
                String win = parts[3].trim();

                // Clé : Source | Valeur : EDGE;Target;Count;Win
                context.write(new Text(source), new Text("EDGE;" + target + ";" + count + ";" + win));
            }
        }

        public static class JoinReducer extends Reducer<Text, Text, Text, NullWritable> {
            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                String countSource = null;
                List<String> edges = new ArrayList<>();

                for (Text val : values) {
                    String s = val.toString();
                    if (s.startsWith("NODE;")) {
                        countSource = s.substring(5);
                    } else if (s.startsWith("EDGE;")) {
                        edges.add(s.substring(5));
                    }
                }

                if (countSource != null) {
                    for (String edge : edges) {
                        // edge contient : Target;Count;Win
                        // On émet : Source;Target;Count;Win;CountSource
                        String out = key.toString() + ";" + edge + ";" + countSource;
                        context.write(new Text(out), NullWritable.get());
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
                // reçoit : Archetype;Count;Win
                String line = value.toString().replaceAll("\t", ";");
                String[] parts = line.split(";");
                
                if (parts.length >= 2) {
                    String archetype = parts[0].trim();
                    String count = parts[1].trim();
                    context.write(new Text(archetype), new Text("NODE;" + count));
                }
            }
        }

        public static class EdgeMapper extends Mapper<Object, Text, Text, Text> {
            @Override
            protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                // reçoit : Source;Target;Count;Win;CountSource
                String line = value.toString(); 
                String[] parts = line.split(";");

                if (parts.length < 5) return;

                String target = parts[1].trim();
                
                // Clé : Target | Valeur : EDGE;Source;Target;Count;Win;CountSource
                context.write(new Text(target), new Text("EDGE;" + line));
            }
        }

        public static class JoinReducer extends Reducer<Text, Text, Text, NullWritable> {
            long nAll = 1;

            // récupère nAll pour calculer la prévision
            @Override
            protected void setup(Context context) {
                nAll = context.getConfiguration().getLong("nAll", 1);
                if (nAll == 0) nAll = 1;
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
                    for (String edge : edges) {
                        String[] parts = edge.split(";");
                        if (parts.length < 5) continue;

                        String source = parts[0];
                        String target = parts[1];
                        long count = Long.parseLong(parts[2]);
                        long win = Long.parseLong(parts[3]);
                        long countSource = Long.parseLong(parts[4]);

                        double prevision = (double) (countSource * countTarget) / nAll;

                        // SORTIE FINALE
                        String out = source + ";" + target + ";" + count + ";" + win + ";" + 
                                     countSource + ";" + countTarget + ";" + String.format("%.2f", prevision).replace(',', '.');
                        
                        context.write(new Text(out), NullWritable.get());
                    }
                }
            }
        }
    }

    // --- Main job runner ---
    public static boolean runJob(String nodesPath, String edgesPath, String outputPath, long nAll) throws Exception {
        Configuration conf = new Configuration();
        
        Path outPath = new Path(outputPath);
        String safeOutputName = outPath.getName();
        Path tempPath = new Path(outPath.getParent(), safeOutputName + "_temp_job1");

        System.out.println(">>> Stats Job 1: Jointure Source...");
        System.out.println("    Input Nodes: " + nodesPath);
        System.out.println("    Input Edges: " + edgesPath);
        System.out.println("    Temp Path: " + tempPath.toString());
        
        // --- JOB 1 ---
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
        
        // --- JOB 2 ---
        System.out.println(">>> Stats Job 2: Jointure Target...");
        
        Configuration conf2 = new Configuration();
        conf2.setLong("nAll", nAll);
        Job job2 = Job.getInstance(conf2, "Stats - Join Target");
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
        
        if (success) {
            fs.delete(tempPath, true);
            System.out.println(">>> TERMINÉ ! Vérifie le fichier dans : " + outputPath + "/part-r-00000");
        }
        
        return success;
    }
}