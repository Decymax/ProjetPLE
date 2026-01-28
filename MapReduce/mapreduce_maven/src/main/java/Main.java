import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            printUsage();
            System.exit(1);
        }

        String task = args[0].toLowerCase();
        boolean success = false;
        
        // --- RECUPERATION ROBUSTE DE LA TAILLE (k) ---
        int size = 8; // Valeur par défaut
        for (String arg : args) {
            if (arg.startsWith("--size=")) {
                try {
                    size = Integer.parseInt(arg.substring(7));
                } catch (NumberFormatException e) {
                    System.err.println("Format de taille incorrect, utilisation de 8 par défaut.");
                }
            }
        }

        switch (task) {
            case "clean":
                success = runDataCleaning(args);
                break;

            case "nodes":
                success = runNodesAndEdges(args, size);
                break;
            
            case "stats":
                success = runStats(args, size);
                break;
            
            case "all":
                // Pipeline complet (optionnel)
                if(runDataCleaning(args)) {
                   if(runNodesAndEdges(new String[]{args[0], "cleaned", "nodes_out"}, size)) {
                       // ... logique all ...
                   }
                }
                break;

            default:
                System.err.println("Erreur: Tâche inconnue '" + task + "'");
                printUsage();
                System.exit(1);
        }
        
        System.exit(success ? 0 : 1);
    }

    private static boolean runDataCleaning(String[] args) throws Exception {
        if (args.length < 3) return false;
        prepareOutput(args[2]);
        System.out.println(">>> Démarrage DataCleaning...");
        return DataCleaning.runJob(new String[]{args[1], args[2]});
    }

    private static boolean runNodesAndEdges(String[] args, int size) throws Exception {
        if (args.length < 3) return false;
        prepareOutput(args[2]);
        System.out.println(">>> Démarrage NodesAndEdges (size=" + size + ")...");
        return NodesAndEdges.runJob(new String[]{args[1], args[2]}, size);
    }

    private static boolean runStats(String[] args, int size) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: stats <nodes_path> <edges_path> <output>");
            return false;
        }
        
        String nodesPath = args[1];
        String edgesPath = args[2];
        String outputPath = args[3];

        long nAll = calculateNAll(edgesPath);
        
        prepareOutput(outputPath);
        
        System.out.println(">>> Démarrage Stats (nAll=" + nAll + ", size=" + size + ")...");
        return Stats.runJob(nodesPath, edgesPath, outputPath, nAll, size);
    }
    
    /**
     * Calcule N_ALL en sommant la colonne 'Count' du fichier EDGES.
     */
    private static long calculateNAll(String edgesPath) throws Exception {
        Configuration conf = new Configuration();
        Path path = new Path(edgesPath);
        FileSystem fs = path.getFileSystem(conf);
        
        long total = 0;
        
        if (fs.isDirectory(path)) {
            path = new Path(edgesPath + "/part-*");
        }

        try {
            org.apache.hadoop.fs.FileStatus[] stats = fs.globStatus(path);
            if (stats == null) return 1;

            for (org.apache.hadoop.fs.FileStatus stat : stats) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(stat.getPath())))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split(";");
                        if (parts.length >= 3) {
                            // Format: Source;Target;Count;Wins
                            total += Long.parseLong(parts[2]);
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Attention: Impossible de calculer nAll, utilisation de 1.");
            return 1;
        }
        return total > 0 ? total : 1;
    }

    private static void prepareOutput(String pathStr) throws Exception {
        Configuration conf = new Configuration();
        Path path = new Path(pathStr);
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path)) {
            System.out.println("Suppression du dossier existant : " + pathStr);
            fs.delete(path, true);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: hadoop jar projet.jar Main <task> <args> [--size=k]");
    }
}