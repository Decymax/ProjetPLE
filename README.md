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
```

```bash
cd /net/cremi/luautret/espaces/travail/m2/ProgLarge/ProjetPLE/MapReduce/mapreduce_maven && rm -rf output_local && mvn exec:java -Dexec.mainClass="DataCleaning" -Dexec.args="../raw_data_100K.json output_local" 2>&1 | tail -50
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

