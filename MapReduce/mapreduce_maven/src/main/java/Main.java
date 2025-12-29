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
        
        switch (task) {
            case "clean":
                success = runDataCleaning(args);
                break;

            case "nodes":
                success = runNodesAndEdges(args);
                break;
            
            case "all":
                success = runFullPipeline(args);
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

    private static boolean runDataCleaning(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: clean <input> <output>");
            return false;
        }
        prepareOutput(args[2]);
        return DataCleaning.runJob(new String[]{args[1], args[2]});
    }

    private static boolean runNodesAndEdges(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: nodes <input_cleaned> <output> [--size=8]");
            return false;
        }
        
        // Parser la taille d'archétype (optionnel)
        int size = 8;
        for (int i = 3; i < args.length; i++) {
            if (args[i].startsWith("--size=")) {
                size = Integer.parseInt(args[i].substring(7));
            }
        }
        
        prepareOutput(args[2]);
        return NodesAndEdges.runJob(new String[]{args[1], args[2]}, size);
    }
    
    private static boolean runFullPipeline(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: all <input_raw> <output_final> [--size=8]");
            return false;
        }
        
        // Parser la taille d'archétype
        int size = 8;
        for (int i = 3; i < args.length; i++) {
            if (args[i].startsWith("--size=")) {
                size = Integer.parseInt(args[i].substring(7));
            }
        }
        
        String inputRaw = args[1];
        String outputFinal = args[2];
        String tempCleaned = outputFinal + "_temp_cleaned";

        System.out.println(">>> ÉTAPE 1 : Nettoyage...");
        prepareOutput(tempCleaned);
        boolean cleanSuccess = DataCleaning.runJob(new String[]{inputRaw, tempCleaned});

        if (!cleanSuccess) {
            System.err.println("!!! Échec du nettoyage.");
            return false;
        }

        System.out.println(">>> ÉTAPE 2 : Génération Nodes & Edges (size=" + size + ")...");
        prepareOutput(outputFinal);
        boolean nodesSuccess = NodesAndEdges.runJob(new String[]{tempCleaned, outputFinal}, size);

        if (nodesSuccess) {
             System.out.println(">>> Nettoyage des fichiers temporaires...");
             Configuration conf = new Configuration();
             FileSystem.get(conf).delete(new Path(tempCleaned), true);
        }

        return nodesSuccess;
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
        System.out.println("\n===========================================");
        System.out.println("  Projet MapReduce - Clash Royale");
        System.out.println("===========================================");
        System.out.println("Usage: hadoop jar projet.jar Main <task> <input> <output> [options]\n");
        System.out.println("Tâches:");
        System.out.println("  clean <input> <output>              - Nettoyage JSON et doublons");
        System.out.println("  nodes <input> <output> [--size=k]   - Génération Nœuds et Arêtes");
        System.out.println("  all   <input> <output> [--size=k]   - Pipeline complet (clean + nodes)");
        System.out.println("\nOptions:");
        System.out.println("  --size=k  : Taille des archétypes (1-8, défaut=8 = deck complet)");
        System.out.println("\nExemples:");
        System.out.println("  Main clean raw_data.json cleaned/");
        System.out.println("  Main nodes cleaned/ output/ --size=4");
        System.out.println("  Main all raw_data.json output/ --size=8");
    }
}
