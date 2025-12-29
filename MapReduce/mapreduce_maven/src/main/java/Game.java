import java.util.List;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Arrays;

public class Game {
    private String date;
    private String game;
    private int round;
    private int winner;
    private List<Player> players;
    
    private String mode;
    private String type;

    public Game() {}

    /**
     * Vérifie si les données du jeu sont valides.
     */
    public boolean isValid() {
        if (date == null || date.isEmpty()) return false;
        if (game == null || game.isEmpty()) return false;
        if (players == null || players.size() != 2) return false;

        for (Player player : players) {
            if (!player.isValid()) return false;
        }
        return true;
    }
    
    public int getWinner() { return winner; }
    
    public List<Player> getPlayers() { return players; }

    /**
     * Génère une clé basée UNIQUEMENT sur les joueurs et le round.
     * Cela permet d'envoyer toutes les parties (A vs B) au même Reducer, 
     * peu importe l'heure ou le décalage de secondes.
     */
    public String getPlayerPairKey() {
        if (players == null || players.size() != 2) return null;
        
        String tag1 = players.get(0).getUtag();
        String tag2 = players.get(1).getUtag();
        
        // Ordre alphabétique pour gérer A vs B == B vs A
        String minTag = (tag1.compareTo(tag2) < 0) ? tag1 : tag2;
        String maxTag = (tag1.compareTo(tag2) < 0) ? tag2 : tag1;
        
        return minTag + "|" + maxTag + "|" + round;
    }

    /**
     * Convertit la date string en millisecondes pour le tri chronologique.
     */
    public long getTimestampMillis() {
        try {
            if (date == null) return 0;
            // Nettoyage format ISO (ex: 2025-11-11T15:47:37Z -> 2025-11-11T15:47:37)
            String cleanDate = date.replace("Z", ""); 
            if (cleanDate.length() > 19) cleanDate = cleanDate.substring(0, 19);
            
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            return sdf.parse(cleanDate).getTime();
        } catch (Exception e) {
            return 0;
        }
    }
}