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

        switch (task) {
            case "clean":
                runDataCleaning(args);
                break;

            case "help":
            case "-h":
            case "--help":
                printUsage();
                break;

            default:
                System.err.println("Erreur: Tâche inconnue '" + task + "'");
                printUsage();
                System.exit(1);
        }
    }

    private static void runDataCleaning(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Erreur: La tâche 'clean' nécessite 2 arguments: <input_path> <output_path>");
            System.err.println("Usage: java Main clean <input_path> <output_path>");
            System.exit(1);
        }

        String[] cleanArgs = new String[] { args[1], args[2] };
        
        Configuration conf = new Configuration();
        Path outputPath = new Path(args[2]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            System.out.println("Le dossier '" + args[2] + "' existe déjà, suppression...");
            fs.delete(outputPath, true);
        }
        
        System.out.println("===========================================");
        System.out.println("  Lancement du Data Cleaning");
        System.out.println("  Input:  " + args[1]);
        System.out.println("  Output: " + args[2]);
        System.out.println("===========================================\n");

        DataCleaning.main(cleanArgs);
    }

    private static void printUsage() {
        System.out.println("\n===========================================");
        System.out.println("  Projet MapReduce - Clash Royale");
        System.out.println("===========================================");
        System.out.println("\nUsage: java Main <task> [arguments...]\n");
        System.out.println("Tâches disponibles:");
        System.out.println("  clean <input> <output>  - Nettoyage des données brutes");
        System.out.println("                            Supprime les JSON invalides et les doublons");
        System.out.println("  help                    - Affiche cette aide\n");
        System.out.println("Exemples:");
        System.out.println("  java Main clean raw_data.json output_clean/");
        System.out.println("  hadoop jar monjar.jar Main clean hdfs://input hdfs://output\n");
    }
}
