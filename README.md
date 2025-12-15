### Projet PLE - MapReduce (Clash Royale)

## Récuperer dataset

```bash
hdfs dfs -ls /user/auber/data_ple/clash_royale
```

## Compiler le projet

```bash
mvn clean compile package
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
