public class Player {
    private String utag;
    private int trophies;
    private String deck;

    private String ctag;
    private int exp;
    private int league;
    private int bestleague;
    private String evo;
    private String tower;
    private double strength;
    private int crown;
    private double elixir;
    private int touch;
    private int score;

    public Player() {}

    /**
     * Vérifie si les données du joueur sont valides.
     */
    public boolean isValid() {
        if (utag == null || utag.isEmpty()) return false;
        
        // Validation Deck: 16 chars hexa (8 cartes * 2)
        if (deck == null || deck.length() != 16) return false;
        if (!deck.matches("[0-9a-fA-F]+")) return false;
        
        return true;
    }

    public String getUtag() { return utag; }
    
    public String getDeck() { return deck; }
    
    /**
     * Retourne les 8 cartes du deck sous forme de tableau.
     * Chaque carte est représentée par 2 caractères hexa.
     */
    public String[] getCards() {
        if (deck == null || deck.length() != 16) return null;
        String[] cards = new String[8];
        for (int i = 0; i < 8; i++) {
            cards[i] = deck.substring(i * 2, i * 2 + 2);
        }
        return cards;
    }
}