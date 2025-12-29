import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class DataCleaning {

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

      // Utilisation de la clé "Paire de Joueurs" (sans date)
      String pairKey = game.getPlayerPairKey();
      
      if (pairKey == null) {
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

      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          List<Game> games = new ArrayList<>();

          // Désérialiser toutes les parties reçues pour ce couple de joueurs
          for (Text val : values) {
              try {
                  games.add(gson.fromJson(val.toString(), Game.class));
              } catch (Exception e) {
                  // Ignorer les erreurs de désérialisation
              }
          }

          if (games.isEmpty()) return;

          // Trier par date précise (Timestamp)
          Collections.sort(games, new Comparator<Game>() {
              @Override
              public int compare(Game g1, Game g2) {
                  return Long.compare(g1.getTimestampMillis(), g2.getTimestampMillis());
              }
          });

          // Filtrage avec fenêtre de temps (10 secondes)
          
          Game lastValidGame = games.get(0);
          context.write(new Text(gson.toJson(lastValidGame)), NullWritable.get());
          context.getCounter(DataCounters.OUTPUT_LINES).increment(1);

          for (int i = 1; i < games.size(); i++) {
              Game currentGame = games.get(i);
              
              long timeDiff = Math.abs(currentGame.getTimestampMillis() - lastValidGame.getTimestampMillis());
              
              // Si écart < 10 secondes, on considère comme doublon
              if (timeDiff < 10000) {
                  context.getCounter(DataCounters.DUPLICATES).increment(1);
              } else {
                  // C'est une vraie nouvelle partie (Revanche), on garde
                  context.write(new Text(gson.toJson(currentGame)), NullWritable.get());
                  context.getCounter(DataCounters.OUTPUT_LINES).increment(1);
                  lastValidGame = currentGame;
              }
          }
      }
  }

  // --- Main Job Runner ---
  public static boolean runJob(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "PLE Clash Royale - Data Cleaning");
      
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