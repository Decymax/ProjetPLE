import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.gson.Gson;

/**
 * Job MapReduce pour générer les nœuds (archétypes) et arêtes (matchups).
 * 
 * Supporte les archétypes de taille k (1 à 8 cartes).
 * Utilise un Combiner pour réduire le trafic réseau.
 * Utilise MultipleOutputs pour écrire dans 2 fichiers (nodes/edges).
 */
public class NodesAndEdges {

    public static final String ARCHETYPE_SIZE_KEY = "archetype.size";
    public static final int DEFAULT_ARCHETYPE_SIZE = 8;

    public enum Counters {
        GAMES_PROCESSED, INVALID_GAMES,
        // Mapper
        MAPPER_NODES_EMITTED, MAPPER_EDGES_EMITTED,
        // Combiner
        COMBINER_NODES_EMITTED, COMBINER_EDGES_EMITTED,
        // Reducer (final)
        REDUCER_NODES_WRITTEN, REDUCER_EDGES_WRITTEN
    }

    // --- MAPPER ---
    public static class ArchetypeMapper extends Mapper<Object, Text, Text, Text> {
        private static final Gson gson = new Gson();
        private int archetypeSize;

        // Initialisation du Mapper (pernmet de configurer la taille des archétypes)
        @Override
        protected void setup(Context context) {
            archetypeSize = context.getConfiguration().getInt(ARCHETYPE_SIZE_KEY, DEFAULT_ARCHETYPE_SIZE);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Game game;
            try {
                game = gson.fromJson(value.toString(), Game.class);
            } catch (Exception e) {
                context.getCounter(Counters.INVALID_GAMES).increment(1);
                return;
            }

            if (game == null || game.getPlayers() == null || game.getPlayers().size() != 2) {
                context.getCounter(Counters.INVALID_GAMES).increment(1);
                return;
            }

            context.getCounter(Counters.GAMES_PROCESSED).increment(1);

            Player p0 = game.getPlayers().get(0);
            Player p1 = game.getPlayers().get(1);
            int winner = game.getWinner(); // 0 = joueur 0 gagne, 1 = joueur 1 gagne

            String[] cards0 = p0.getCards();
            String[] cards1 = p1.getCards();

            if (cards0 == null || cards1 == null) {
                context.getCounter(Counters.INVALID_GAMES).increment(1);
                return;
            }

            // Trier les cartes pour avoir une représentation canonique
            Arrays.sort(cards0);
            Arrays.sort(cards1);

            // Deck complet (pour les arêtes)
            String fullDeck0 = String.join("", cards0);
            String fullDeck1 = String.join("", cards1);

            // Générer tous les archétypes de taille k (pour les nœuds)
            List<String> archetypes0 = generateArchetypes(cards0, archetypeSize);
            List<String> archetypes1 = generateArchetypes(cards1, archetypeSize);

            // --- ÉMETTRE LES NŒUDS ---
            int win0 = (winner == 0) ? 1 : 0;
            for (String arch : archetypes0) {
                // Format: "N|archetype" -> "count,wins"
                context.write(new Text("N|" + arch), new Text("1," + win0));
                context.getCounter(Counters.MAPPER_NODES_EMITTED).increment(1);
            }

            int win1 = (winner == 1) ? 1 : 0;
            for (String arch : archetypes1) {
                context.write(new Text("N|" + arch), new Text("1," + win1));
                context.getCounter(Counters.MAPPER_NODES_EMITTED).increment(1);
            }

            // --- ÉMETTRE LES ARÊTES ---
            context.write(new Text("E|" + fullDeck0 + "|" + fullDeck1), new Text("1," + win0));
            context.getCounter(Counters.MAPPER_EDGES_EMITTED).increment(1);
            
            context.write(new Text("E|" + fullDeck1 + "|" + fullDeck0), new Text("1," + win1));
            context.getCounter(Counters.MAPPER_EDGES_EMITTED).increment(1);
        }

        /**
         * Génère toutes les combinaisons de k cartes parmi les 8 cartes du deck.
         * Les cartes sont triées pour avoir une clé canonique.
         */
        private List<String> generateArchetypes(String[] cards, int k) {
            List<String> result = new ArrayList<>();
            
            if (k == 8) {
                result.add(String.join("", cards));
            } else {
                generateCombinations(cards, k, 0, new String[k], 0, result);
            }
            
            return result;
        }

        /**
         * Génère récursivement les combinaisons.
         */
        private void generateCombinations(String[] cards, int k, int start, 
                                          String[] current, int index, List<String> result) {
            if (index == k) {
                // On a k cartes, créer l'archétype
                result.add(String.join("", current));
                return;
            }
            
            for (int i = start; i <= cards.length - (k - index); i++) {
                current[index] = cards[i];
                generateCombinations(cards, k, i + 1, current, index + 1, result);
            }
        }
    }

    // --- COMBINER ---
    public static class ArchetypeCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            long totalCount = 0;
            long totalWins = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                totalCount += Long.parseLong(parts[0]);
                totalWins += Long.parseLong(parts[1]);
            }

            context.write(key, new Text(totalCount + "," + totalWins));
            
            // Compteur pour voir l'effet du Combiner
            String keyStr = key.toString();
            if (keyStr.startsWith("N|")) {
                context.getCounter(Counters.COMBINER_NODES_EMITTED).increment(1);
            } else if (keyStr.startsWith("E|")) {
                context.getCounter(Counters.COMBINER_EDGES_EMITTED).increment(1);
            }
        }
    }

    // --- REDUCER ---
    public static class ArchetypeReducer extends Reducer<Text, Text, Text, Text> {
        private MultipleOutputs<Text, Text> multipleOutputs;

        @Override
        protected void setup(Context context) {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            long totalCount = 0;
            long totalWins = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                totalCount += Long.parseLong(parts[0]);
                totalWins += Long.parseLong(parts[1]);
            }

            String keyStr = key.toString();
            
            if (keyStr.startsWith("N|")) {
                String archetype = keyStr.substring(2);
                // archetype;count;wins
                String output = archetype + ";" + totalCount + ";" + totalWins;
                multipleOutputs.write("nodes", new Text(output), new Text(""));
                context.getCounter(Counters.REDUCER_NODES_WRITTEN).increment(1);
                
            } else if (keyStr.startsWith("E|")) {
                String[] parts = keyStr.substring(2).split("\\|");
                String source = parts[0];
                String target = parts[1];
                // source;target;count;wins
                String output = source + ";" + target + ";" + totalCount + ";" + totalWins;
                multipleOutputs.write("edges", new Text(output), new Text(""));
                context.getCounter(Counters.REDUCER_EDGES_WRITTEN).increment(1);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    // --- Main Job Runner ---
    public static boolean runJob(String[] args) throws Exception {
        return runJob(args, DEFAULT_ARCHETYPE_SIZE);
    }

    public static boolean runJob(String[] args, int archetypeSize) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt(ARCHETYPE_SIZE_KEY, archetypeSize);

        Job job = Job.getInstance(conf, "PLE Clash Royale - Nodes & Edges (size=" + archetypeSize + ")");

        job.setJarByClass(NodesAndEdges.class);
        job.setMapperClass(ArchetypeMapper.class);
        job.setCombinerClass(ArchetypeCombiner.class);
        job.setReducerClass(ArchetypeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleOutputs.addNamedOutput(job, "nodes", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "edges", TextOutputFormat.class, Text.class, Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        if (success) {
            org.apache.hadoop.mapreduce.Counters c = job.getCounters();
            
            long mapperNodes = c.findCounter(Counters.MAPPER_NODES_EMITTED).getValue();
            long mapperEdges = c.findCounter(Counters.MAPPER_EDGES_EMITTED).getValue();
            long combinerNodes = c.findCounter(Counters.COMBINER_NODES_EMITTED).getValue();
            long combinerEdges = c.findCounter(Counters.COMBINER_EDGES_EMITTED).getValue();
            long reducerNodes = c.findCounter(Counters.REDUCER_NODES_WRITTEN).getValue();
            long reducerEdges = c.findCounter(Counters.REDUCER_EDGES_WRITTEN).getValue();
            
            System.out.println("\n-------------------------------------------");
            System.out.println("  RAPPORT NODES & EDGES");
            System.out.println("-------------------------------------------");
            System.out.println("  Taille archétype    : " + archetypeSize);
            System.out.println("  Parties traitées    : " + c.findCounter(Counters.GAMES_PROCESSED).getValue());
            System.out.println("  Parties invalides   : " + c.findCounter(Counters.INVALID_GAMES).getValue());
            System.out.println("-------------------------------------------");
            System.out.println("  MAPPER  → Nœuds émis : " + mapperNodes);
            System.out.println("  MAPPER  → Arêtes     : " + mapperEdges);
            System.out.println("-------------------------------------------");
            System.out.println("  COMBINER→ Nœuds émis : " + combinerNodes);
            System.out.println("  COMBINER→ Arêtes     : " + combinerEdges);
            if (mapperNodes > 0) {
                double nodeReduction = (1.0 - (double)combinerNodes / mapperNodes) * 100;
                double edgeReduction = (1.0 - (double)combinerEdges / mapperEdges) * 100;
                System.out.println("  Réduction nœuds      : " + String.format("%.1f%%", nodeReduction));
                System.out.println("  Réduction arêtes     : " + String.format("%.1f%%", edgeReduction));
            }
            System.out.println("-------------------------------------------");
            System.out.println("  REDUCER → Nœuds écrits: " + reducerNodes);
            System.out.println("  REDUCER → Arêtes      : " + reducerEdges);
            System.out.println("-------------------------------------------\n");
        }

        return success;
    }

    public static void main(String[] args) throws Exception {
        int size = DEFAULT_ARCHETYPE_SIZE;
        
        // Parser --size=X si présent
        for (String arg : args) {
            if (arg.startsWith("--size=")) {
                size = Integer.parseInt(arg.substring(7));
            }
        }
        
        System.exit(runJob(args, size) ? 0 : 1);
    }
}
