import java.io.IOException;
import java.util.Arrays;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonSyntaxException;

public class DataCleaning {
  public static class DataCleaningMapper extends Mapper<Object, Text, Text, Text> {

    private static final Gson gson = new Gson();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      String line = value.toString();

      JsonObject jObject;
      try {
        jObject = gson.fromJson(line, JsonObject.class);
      } catch (JsonSyntaxException e) {
        return; // JSON invalide
      }

      // Vérifier les champs obligatoires
      if(!jObject.has("date")) return;
      if(!jObject.has("game")) return;
      if(!jObject.has("winner")) return;
      if(!jObject.has("round")) return;
      if(!jObject.has("players")) return;

      JsonArray players = jObject.getAsJsonArray("players");
      if (players.size() != 2) return;

      // Vérifier chaque joueur
      String utag1 = null, utag2 = null;
      for (int i = 0; i < players.size(); i++) {
        JsonObject player = players.get(i).getAsJsonObject();
        if(!player.has("utag")) return;
        if(!player.has("trophies")) return;
        if(!player.has("deck")) return;
        
        // Vérifier que le deck a exactement 8 cartes (16 caractères hex)
        String deck = player.get("deck").getAsString();
        if (deck == null || deck.length() != 16) return;
        
        if (i == 0) utag1 = player.get("utag").getAsString();
        else utag2 = player.get("utag").getAsString();
      }

      // Créer une clé unique pour détecter les doublons
      // Format: date_arrondie|utag_min|utag_max|round
      String dateStr = jObject.get("date").getAsString();
      String roundedDate = roundDateToMinute(dateStr);
      int round = jObject.get("round").getAsInt();
      
      // Trier les utags pour avoir un ordre canonique
      String[] utags = {utag1, utag2};
      Arrays.sort(utags);
      
      String uniqueKey = roundedDate + "|" + utags[0] + "|" + utags[1] + "|" + round;
      
      // Émettre la clé unique et la ligne complète
      context.write(new Text(uniqueKey), new Text(line));
    }
    
    // Arrondir la date à la minute pour tolérer quelques secondes de différence
    private String roundDateToMinute(String dateStr) {
      try {
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
        Date date = inputFormat.parse(dateStr);
        return outputFormat.format(date);
      } catch (Exception e) {
        return dateStr.substring(0, 16); // fallback: garder YYYY-MM-DDTHH:mm
      }
    }
  }

  public static class DataCleaningReducer extends Reducer<Text,Text,Text,IntWritable> {
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          // Ne garder que la première occurrence de chaque partie unique
          boolean first = true;
          for (Text value : values) {
              if (first) {
                  context.write(value, null);
                  first = false;
              }
          }
      }
  }

  public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "Data Cleaning");
      
      job.setJarByClass(DataCleaning.class);
      job.setMapperClass(DataCleaningMapper.class);
      job.setReducerClass(DataCleaningReducer.class);
      
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
      System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}