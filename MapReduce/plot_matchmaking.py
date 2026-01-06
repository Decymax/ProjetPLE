import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
import os
import sys

FILE_NAME = "result_hdfs/output_stats/part-r-00000"

def analyser_resultats():
    print("==========================================")
    print("   ANALYSE MATCHMAKING CLASH ROYALE")
    print("==========================================")

    if not os.path.exists(FILE_NAME):
        print(f"ERREUR : Le fichier '{FILE_NAME}' est introuvable.")
        print("Assure-toi de l'avoir récupéré depuis Hadoop :")
        print(f"  hadoop fs -get ./output_stats/{FILE_NAME} .")
        return

    print(f">>> Chargement de '{FILE_NAME}'...")
    
    try:
        df = pd.read_csv(FILE_NAME, sep=";", header=None, 
                         names=["Source", "Target", "CountObs", "Win", 
                                "CountSource", "CountTarget", "Prevision"])
    except Exception as e:
        print(f"ERREUR lors de la lecture du CSV : {e}")
        return

    total_lines = len(df)
    print(f"  - Total lignes chargées : {total_lines}")

    df_clean = df[df['Prevision'] > 0.01].copy()
    
    useful_lines = len(df_clean)
    print(f"  - Lignes exploitables (Prevision > 0.01) : {useful_lines}")
    
    if useful_lines < 2:
        print("\nERREUR : Pas assez de données significatives pour tracer le graphique.")
        print("Conseil : Vérifie que ton dataset contient des decks populaires.")
        return

    x = df_clean['Prevision']
    y = df_clean['CountObs']

    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
    r_squared = r_value ** 2

    print("\n>>> RÉSULTATS STATISTIQUES")
    print(f"  - Pente (Slope) : {slope:.4f}")
    print(f"  - R²            : {r_squared:.4f}")
    print(f"  - Intercept     : {intercept:.4f}")

    print("\n>>> INTERPRÉTATION RAPIDE")
    if 0.9 <= slope <= 1.1:
        print("  [OK] Pente proche de 1.0 : Le matchmaking semble ALÉATOIRE (Honnête).")
    elif 0.45 <= slope <= 0.55:
        print("  [ATTENTION] Pente proche de 0.5 : Tu as probablement oublié le facteur *2 dans la formule Java.")
        print("  Correctif : double prevision = (countSource * countTarget) / (2.0 * nAll);")
    else:
        print(f"  [SUSPECT] La pente ({slope:.2f}) s'éloigne de 1. Il y a un biais (Attraction ou Évitement).")

    print("\n>>> Génération du graphique...")
    plt.figure(figsize=(10, 7))

    plt.scatter(x, y, alpha=0.4, s=15, color='blue', label='Paires de Decks')

    max_val = max(x.max(), y.max())
    plt.plot([0, max_val], [0, max_val], color='red', linestyle='--', linewidth=2, label='Idéal Théorique (y=x)')

    plt.plot(x, slope*x + intercept, color='green', linewidth=2, label=f'Réalité (Pente={slope:.3f})')

    plt.title(f"Analyse du Matchmaking Clash Royale\nPente: {slope:.3f} (Attendue: 1.0) | R²: {r_squared:.3f}")
    plt.xlabel("Nombre de rencontres THÉORIQUE")
    plt.ylabel("Nombre de rencontres RÉEL (Observé)")
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.6)

    output_img = "graphique_matchmaking.png"
    plt.savefig(output_img)
    print(f">>> Graphique sauvegardé sous '{output_img}'")
    
    try:
        plt.show()
    except:
        pass

if __name__ == "__main__":
    analyser_resultats()