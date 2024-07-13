package mg.lahatra3;

public class WisedataJob {
    public static void main(String[] args) {
        System.out.println("Start processing at " + System.currentTimeMillis() + " ...");
        WisedataJobService wisedataJobService = new WisedataJobService();
        wisedataJobService.process();
        System.out.println("Finish processing at " + System.currentTimeMillis() + " ...");
    }
}