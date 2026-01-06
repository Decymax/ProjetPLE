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
java -jar target/clash-royale-0.0.1.jar clean ../raw_data_100K.json ./output_clean/
java -jar target/clash-royale-0.0.1.jar nodes ./output_clean/part-r-00000 ../output_nodes/ --size=6
java -jar target/clash-royale-0.0.1.jar stats ./output_nodes/nodes-r-00000 ./output_nodes/edges-r-00000 ./output_stats/
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

# Data Cleaning MapReduce

- Pas de doublons exactes
- Pas de parties enregistrées plusieurs fois
- Deck avec 8 cartes
- Lignes formatées correctement en JSON

1085 lignes dupliquées supprimées dans le dataset de 100k lignes.
Le fichier de sortie du 100k lignes doit être le même que le fichier de 200k lignes après nettoyage.

## Exemple de ligne

{"date":"2025-11-11T15:47:37Z","game":"pathOfLegend","mode":"Ranked1v1_NewArena2","round":0,"type":"pathOfLegend","winner":0,"players":[{"utag":"#CJR2U0P80","ctag":"#2L022JC0","trophies":10000,"exp":56,"league":4,"bestleague":8,"deck":"0a0e151e264d5a65","evo":"0a0e","tower":"6e","strength":15.25,"crown":1,"elixir":0.99,"touch":1,"score":0},{"utag":"#U8Y0RY09","ctag":"#YYGV92UQ","trophies":10000,"exp":62,"league":4,"bestleague":10,"deck":"0a1e3b454d65686c","evo":"3b4d","tower":"6e","strength":15.125,"crown":0,"elixir":6.96,"touch":1,"score":0}]}

## Carnet de bord pour le rapport

### Partie 1

Il ne faut pas passer de temps dans le clef lors du mapper. Une même partie peut avoir lieu à 23h59m59s et 00h00m01s à cause des latences serveur, chaque période de temps
heure/minutes/années ect sont donc susceptibles d'être impactés ça. Il faut donc traité ces doublons dans le reducer lorsqu'il reçoit par clef toutes les parties disputés entre
le joueur A et le joueur B.

Quand on compare les clefs des 2 joueurs on s'assure que ces 2 clefs sont triés avant tout traitement ce qui nous permet d'éviter les cas où 2 parties sont identiques mais l'ordre
entre le joueur A et B change. (Partie 1: Joueur A vs joueur B // Partie 1: Joueur B vs joueur A, ces 2 parties sont en réalités identiques).

On utilise Gson pour valider la structure JSON des lignes reçues. Si une ligne n'est pas conforme on l'ignore.

## Partie 2

On génère les noeuds (archétypes de deck) et les arètes (deck entier) à partir des données nettoyées avant.

Un archétypes est une combinaison de n cartes parmis les 8 du deck. Par défaut on met le deck entier, mais on peut choisir une taille plus petite pour avoir plus de 
précision sur les combos de cartes jouées.

On trie les cartes avant pour avoir une représentation unique de l'archétype. Ça évite comme le traitement des joueurs avant de comptabiliser les mêmes archétypes en double.

On utilise un Combiner pour réduire le trafic réseau entre le Mapper et le Reducer. Il s'occupe de calculer le nombre d'occurence des archétypes et leur nombre de victoires
avant d'envoyer les données au Reducer. Ça réduit considérablement le volume de données échangées.

-------------------------------------------
  MAPPER  → Nœuds émis : 196342
  MAPPER  → Arêtes     : 196342
-------------------------------------------
  COMBINER→ Nœuds émis : 67879
  COMBINER→ Arêtes     : 178132
-------------------------------------------

C'est un exemple sur le dataset de 100k lignes nettoyées avec une taille d'archétype de 8 cartes. Par rapport à ce qui sort du mapper on voit que le combiner a réduit le nombre de nœuds émis de 196342 à 67879.

Le reducer va écrire les résultats finaux dans 2 fichiers séparés: un pour les nœuds et un pour les arêtes.
nodes-r-00000 : va renvoyer chaque archétype avec son nombre d'occurrences et son nombre de victoires.
edges-r-00000 : va renvoyer chaque les matchups entre les decks avec le nombre de fois qu'ils se sont affrontés et le nombre de victoires du premier deck contre le second.

format de sortie des nœuds: archétype;count;wins
format de sortie des arêtes: archetype1;archetype2;count;wins

### Partie 3

A partir des 2 ensembles de données qu'on vient de calculer, on doit créer un nouveau fichier qui va contenir pour chaque arête (deck1 vs deck2) la prévision de victoire du deck1 contre le deck2. Les données seront sous cette forme:

Archetype source, Archetype target ; count ; win; count source; count target; prevision
0001;0001;44;19;12051,12051; 39.5
0001;0006;34;17;12051,7840; 30.5
0001;0007;42;18;12051,4700; 47

Pour ce faire on va faire une jointure entre les arêtes et les noeuds pour récupérer le count target (le nombre de parties jouées par le deck cible). 
On doit faire ça en 2 jobs succéssifs, dans le premier on joint les arêtes avec les nœuds pour récupérer le count source (le nombre de parties jouées par le deck source). Dans le second job on joint le résultat du premier job avec les nœuds pour récupérer le count target.