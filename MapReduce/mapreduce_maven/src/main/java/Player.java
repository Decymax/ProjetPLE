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
}