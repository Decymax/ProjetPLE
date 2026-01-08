import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main {

    /**
     * Point d'entrée principal.
     * Arguments:
     *   clean <input> <output>
     *   nodes <input_cleaned> <output> [--size=k]
     *   stats <nodes_file> <edges_file> <output>
     *   all <input_raw> <output_final> [--size=k]
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            printUsage();
            System.exit(1);
        }

        String task = args[0].toLowerCase();
        boolean success = false;
        
        switch (task) {
            case "clean":
                success = runDataCleaning(args);
                break;

            case "nodes":
                success = runNodesAndEdges(args);
                break;
            
            case "stats":
                success = runStats(args);
                break;

            case "help":
            case "-h":
            case "--help":
                printUsage();
                return;

            default:
                System.err.println("Erreur: Tâche inconnue '" + task + "'");
                printUsage();
                System.exit(1);
        }
        
        System.exit(success ? 0 : 1);
    }

    /**
     * Exécute le job de nettoyage des données.
     */
    private static boolean runDataCleaning(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: clean <input> <output>");
            return false;
        }
        prepareOutput(args[2]);
        
        long startTime = System.currentTimeMillis();
        System.out.println(">>> Démarrage du job DataCleaning...");
        
        boolean success = DataCleaning.runJob(new String[]{args[1], args[2]});
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        System.out.println(">>> Job DataCleaning terminé en " + formatDuration(duration));
        
        return success;
    }

    /**
     * Exécute le job de génération des nœuds et arêtes.
     */
    private static boolean runNodesAndEdges(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: nodes <input_cleaned> <output> [--size=8]");
            return false;
        }
        
        // Parser la taille d'archétype (optionnel, 8 par défaut)
        int size = 8;
        for (int i = 3; i < args.length; i++) {
            if (args[i].startsWith("--size=")) {
                size = Integer.parseInt(args[i].substring(7));
            }
        }
        
        prepareOutput(args[2]);
        
        long startTime = System.currentTimeMillis();
        System.out.println(">>> Démarrage du job NodesAndEdges (size=" + size + ")...");
        
        boolean success = NodesAndEdges.runJob(new String[]{args[1], args[2]}, size);
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        System.out.println(">>> Job NodesAndEdges terminé en " + formatDuration(duration));
        
        return success;
    }

    /**
     * Exécute le job de calcul des statistiques avec prévisions.
     * Calcule automatiquement N_ALL en sommant les counts des edges.
     * nécessite d'avoir généré les nœuds et arêtes au préalable (logique)
     */
    private static boolean runStats(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: stats <nodes_file> <edges_file> <output>");
            return false;
        }
        
        String nodesPath = args[1];
        String edgesPath = args[2];
        String outputPath = args[3];
        
        // Calculer nAll automatiquement en sommant les counts des edges
        long nAll = calculateNAll(edgesPath);
        
        prepareOutput(outputPath);
        
        long startTime = System.currentTimeMillis();
        System.out.println(">>> Démarrage du job Stats...");
        
        boolean success = Stats.runJob(nodesPath, edgesPath, outputPath, nAll);
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        System.out.println(">>> Job Stats terminé en " + formatDuration(duration));
        
        return success;
    }
    
    /**
     * Calcule N_ALL en sommant les counts de toutes les edges.
     * Format edge: archetype1;archetype2;count;wins
     * Chaque partie génère 2 edges (A→B et B→A).
     */
    private static long calculateNAll(String edgesPath) throws Exception {
        Configuration conf = new Configuration();
        Path path = new Path(edgesPath);
        FileSystem fs = path.getFileSystem(conf);
        
        long total = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(";");
                if (parts.length >= 3) {
                    total += Long.parseLong(parts[2]);
                }
            }
        }
        return total;
    }

    /**
     * Prépare le dossier de sortie en le supprimant s'il existe déjà.
     * évite les erreurs Hadoop liées à l'existence préalable du dossier.
     */
    private static void prepareOutput(String pathStr) throws Exception {
        Configuration conf = new Configuration();
        Path path = new Path(pathStr);
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path)) {
            System.out.println("Suppression du dossier existant : " + pathStr);
            fs.delete(path, true);
        }
    }

    /**
     * Formate une durée en millisecondes en format lisible.
     */
    private static String formatDuration(long durationMs) {
        long seconds = durationMs / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        
        if (hours > 0) {
            return String.format("%dh %dm %ds (%d ms)", hours, minutes % 60, seconds % 60, durationMs);
        } else if (minutes > 0) {
            return String.format("%dm %ds (%d ms)", minutes, seconds % 60, durationMs);
        } else if (seconds > 0) {
            return String.format("%ds (%d ms)", seconds, durationMs);
        } else {
            return durationMs + " ms";
        }
    }
    
    private static void printUsage() {
        System.out.println("\n===========================================");
        System.out.println("  Projet MapReduce - Clash Royale");
        System.out.println("===========================================");
        System.out.println("Usage: hadoop jar projet.jar Main <task> <input> <output> [options]\n");
        System.out.println("Tâches:");
        System.out.println("  clean <input> <output>              - Nettoyage JSON et doublons");
        System.out.println("  nodes <input> <output> [--size=k]   - Génération Nœuds et Arêtes");
        System.out.println("  stats <nodes> <edges> <output>      - Stats avec prévisions (nAll auto)");
        System.out.println("  all   <input> <output> [--size=k]   - Pipeline complet (clean + nodes)");
        System.out.println("\nOptions:");
        System.out.println("  --size=k  : Taille des archétypes (1-8, défaut=8 = deck complet)");
        System.out.println("\nExemples:");
        System.out.println("  Main clean raw_data.json cleaned/");
        System.out.println("  Main nodes cleaned/ output/ --size=4");
        System.out.println("  Main all raw_data.json output/ --size=8");
    }
}
