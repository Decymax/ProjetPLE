import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
import os
import sys

# Ton fichier d'entrée
FILE_NAME = "result_hdfs/output_stats/part-r-00000"
OUTPUT_IMG = "tableau_de_bord_matchmaking.png"

def analyser_resultats():
    print("==========================================")
    print("   ANALYSE AVANCÉE MATCHMAKING CLASH ROYALE")
    print("==========================================")

    # 1. Chargement des données
    if not os.path.exists(FILE_NAME):
        print(f"ERREUR : Le fichier '{FILE_NAME}' est introuvable.")
        return

    print(f">>> Chargement de '{FILE_NAME}'...")
    try:
        # On définit les noms de colonnes explicitement
        df = pd.read_csv(FILE_NAME, sep=";", header=None, 
                         names=["Source", "Target", "CountObs", "Win", 
                                "CountSource", "CountTarget", "Prevision"])
    except Exception as e:
        print(f"ERREUR lors de la lecture du CSV : {e}")
        return

    # 2. Nettoyage
    # On garde les prévisions > 0.01 pour éviter les divisions par zéro et le bruit
    df_clean = df[df['Prevision'] > 0.01].copy()
    
    useful_lines = len(df_clean)
    print(f" - Lignes exploitables : {useful_lines}")
    
    if useful_lines < 10:
        print("\nERREUR : Pas assez de données pour les stats avancées.")
        return

    # 3. Calculs Statistiques
    x = df_clean['Prevision']  # Théorique
    y = df_clean['CountObs']   # Réel

    # Régression Linéaire
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
    r_squared = r_value ** 2

    # Calcul des Résidus (Ecart entre Réalité et Théorie)
    df_clean["Residus"] = df_clean["CountObs"] - df_clean["Prevision"]
    
    # Z-Score (Résidu normalisé) pour voir la distribution gaussienne
    df_clean["Z_Score"] = (df_clean["Residus"] - df_clean["Residus"].mean()) / df_clean["Residus"].std()
    
    # Erreur Relative (Pourcentage d'erreur)
    df_clean["RelativeError"] = df_clean["Residus"] / df_clean["Prevision"]

    print("\n>>> RÉSULTATS STATISTIQUES")
    print(f" - Pente (Slope) : {slope:.4f} (Idéal: 1.0)")
    print(f" - R²            : {r_squared:.4f}")
    
    # 4. Génération de la planche de 4 graphiques
    print(f"\n>>> Génération du tableau de bord '{OUTPUT_IMG}'...")
    
    # Création d'une figure avec 2 lignes et 2 colonnes
    fig, axs = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle(f'Analyse Complète du Matchmaking Clash Royale\nPente: {slope:.3f} | R²: {r_squared:.3f}', fontsize=16)

    # --- GRAPH 1 : LINÉAIRE (Classique) ---
    ax1 = axs[0, 0]
    ax1.scatter(x, y, alpha=0.4, s=10, color='blue', label='Paires')
    max_val = max(x.max(), y.max())
    ax1.plot([0, max_val], [0, max_val], 'r--', linewidth=2, label='Idéal (y=x)')
    ax1.plot(x, slope*x + intercept, 'g-', linewidth=2, label=f'Réalité (Pente={slope:.2f})')
    ax1.set_title("1. Corrélation Linéaire (Vue d'ensemble)")
    ax1.set_xlabel("Théorique")
    ax1.set_ylabel("Observé")
    ax1.legend()
    ax1.grid(True, linestyle='--', alpha=0.6)

    # --- GRAPH 2 : LOG-LOG (Pour les petites valeurs) ---
    ax2 = axs[0, 1]
    ax2.scatter(x, y, alpha=0.4, s=10, color='purple')
    ax2.plot([0.1, max_val], [0.1, max_val], 'r--', linewidth=2, label='Idéal (y=x)')
    ax2.set_xscale('log')
    ax2.set_yscale('log')
    ax2.set_title("2. Échelle Logarithmique (Cartes rares)")
    ax2.set_xlabel("Théorique (Log)")
    ax2.set_ylabel("Observé (Log)")
    ax2.grid(True, which="both", linestyle='--', alpha=0.6)

    # --- GRAPH 3 : DISTRIBUTION DES RÉSIDUS (Preuve de l'aléatoire) ---
    # On filtre les valeurs extrêmes pour le dessin (entre -4 et +4 sigma)
    z_filtered = df_clean["Z_Score"][df_clean["Z_Score"].between(-4, 4)]
    
    ax3 = axs[1, 0]
    ax3.hist(z_filtered, bins=50, color='orange', edgecolor='black', alpha=0.7, density=True)
    
    # Tracer la courbe de Gauss théorique par dessus
    xmin, xmax = ax3.get_xlim()
    lin_x = np.linspace(xmin, xmax, 100)
    p = stats.norm.pdf(lin_x, 0, 1)
    ax3.plot(lin_x, p, 'k', linewidth=2, label="Gaussienne Normale")
    
    ax3.set_title("3. Distribution des Erreurs (Histogramme)")
    ax3.set_xlabel("Écart Standardisé (Z-Score)")
    ax3.legend()
    ax3.grid(True, alpha=0.3)

    # --- GRAPH 4 : ERREUR RELATIVE vs POPULARITÉ ---
    ax4 = axs[1, 1]
    # On limite l'affichage à +/- 100% d'erreur pour la lisibilité
    ax4.scatter(x, df_clean["RelativeError"], alpha=0.3, s=10, color='teal')
    ax4.axhline(0, color='red', linestyle='--', linewidth=2)
    ax4.set_ylim(-1, 1) 
    ax4.set_title("4. Biais en fonction de la popularité")
    ax4.set_xlabel("Popularité du Matchup (Théorique)")
    ax4.set_ylabel("Erreur Relative")
    ax4.grid(True, alpha=0.6)

    # Sauvegarde
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(OUTPUT_IMG, dpi=300)
    print(f">>> Image sauvegardée : {OUTPUT_IMG}")

    try:
        plt.show()
    except:
        pass

if __name__ == "__main__":
    analyser_resultats()