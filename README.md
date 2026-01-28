### Projet PLE - MapReduce (Clash Royale)

## Récuperer dataset

```bash
hdfs dfs -ls /user/auber/data_ple/clash_royale
```

## Compiler le projet

```bash
mvn clean compile package
```

Pour compiler en local:

```bash
mvn clean package
java -jar target/clash-royale-0.0.1.jar --help
```

Exemple d'execution en local:

```bash
java -jar target/clash-royale-0.0.1.jar clean ../raw_data_100K.json ../result_hdfs/output_clean/
java -jar target/clash-royale-0.0.1.jar nodes ../result_hdfs/output_clean/part-r-00000 ../result_hdfs/output_nodes/ --size=6
java -jar target/clash-royale-0.0.1.jar stats ../result_hdfs/output_nodes/nodes-r-00000 ../result_hdfs/output_nodes/edges-r-00000 ../result_hdfs/output_stats/ --size=6
```

pour tout lancer 
```bash
java -jar target/clash-royale-0.0.1.jar all ../raw_data_100K.json ../result_hdfs/ --size=6
```

Pour envoyer le fichier jar sur la gateway:

```bash
scp target/clash-royale-0.0.1.jar lsd:clash-royale-0.0.1.jar
```

Pour exécuter sur le cluster Hadoop:

```bash
hadoop jar clash-royale-0.0.1.jar clean /user/auber/data_ple/clash_royale/raw_data_100K.json clash-royale/output_clean/
hadoop jar clash-royale-0.0.1.jar nodes clash-royale/output_clean/part-r-00000 clash-royale/output_nodes/ --size=6
hadoop jar clash-royale-0.0.1.jar stats clash-royale/output_nodes/nodes-r-00000 clash-royale/output_nodes/edges-r-00000 clash-royale/output_stats/
```

Pour récupérer les résultats:

```bash
hdfs dfs -get clash-royale/output_* ./resultats/
hdfs dfs -rm -r clash-royale/output_*
```

Pour transférer de la gateway vers la machine locale:

(sur la machine locale)

```bash
scp -r lsd:resultats/ ./resultats/
```

Pour nettoyer la gateway:

```bash
cd ~/resultats/
rm -r output_*
```

Pour récupérer le graphique du matchmaking (Dans ProjetPLE/MapReduce):

```bash
python plot_matchmaking.py
```
