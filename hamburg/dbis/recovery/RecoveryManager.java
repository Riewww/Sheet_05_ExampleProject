package hamburg.dbis.recovery;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;

public class RecoveryManager {

    static final private RecoveryManager _manager;

    // TODO Add class variables if necessary
    public ArrayList<String[]> loglist = new ArrayList<>();

    public ArrayList<ArrayList<String>> userdata = new ArrayList<ArrayList<String>>();
    public LinkedList<Integer> pageids = new LinkedList<Integer>();


    public LinkedList<Integer> winner = new LinkedList<Integer>();

    public LinkedList<String[]> winnerlist = new LinkedList<String[]>();




    static {
        try {
            _manager = new RecoveryManager();
        } catch (Throwable e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private RecoveryManager() {
        // TODO Initialize class variables if necessary
        //1: Make a list of winners out of log.txt-file
        //2: Go through this list: if the user is already in the UserData-folder as a .txt file,
        // check the lsn-number. Bigger one wins. Based on that, either leave it be or overwrite a new .txt file with
        // the latest information

    }

    static public RecoveryManager getInstance() {
        return _manager;
    }

    public void startRecovery() {
        // TODO
        FileReader fileReader = null;


        try {
            String line;
            fileReader = new FileReader("hamburg/dbis/persistence/log.txt");
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while((line=bufferedReader.readLine()) != null){
                loglist.add(line.split("\t"));
            }


            // list of "winner" taaid = []
            // real list of winners -> if line has taaid, which is in taaid -> it's a winner

            for(String[] items : loglist){
                if(items[2].matches("EOT")){
                    winner.add(Integer.parseInt(items[1]));
                }
            }
            // if string does not contain "EOT"
            for(String[] item : loglist){
                if(winner.contains(Integer.parseInt(item[1])) & item[2].matches("EOT") == false ){
                    winnerlist.add(item);
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        // Winners -> lsn , taaid , pageid , data

        // Write out in the userDB as .txt files
        // 1. Loop through files in UserDB -> make a list same as winners, fill missing data with null or something
        // - This so we don't get index out of bounds -error
        // Then compare these together: is user excists, compare lsn numbers, if not, add new to the list with user data
        // (Optional) sort with page numbers ascending
        // Write all files in UserDB. If .txt file exists for that user, overwrite it. Otherwise write a new .txt file


        File folder = new File("hamburg/dbis/UserData");
        File[] files = folder.listFiles();

        for(File file: files){
            String line;
            try {
                fileReader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                while((line=bufferedReader.readLine()) != null){
                    ArrayList<String> list = new ArrayList<>();
                    list.add(file.getName());
                    list.add(line.split(",")[0]);
                    list.add(line.split(",")[1]);
                    userdata.add(list);
                    pageids.add(Integer.parseInt(file.getName()));
                }
                fileReader.close();
                bufferedReader.close();
                // userdata List: pageid, lsn, data
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        for(String[] winner : winnerlist) {
            Boolean noFound = true;
            for(ArrayList<String> user: userdata){
                if(Integer.parseInt(user.get(0)) == Integer.parseInt(winner[2])){
                    if(Integer.parseInt(user.get(1)) < Integer.parseInt(winner[0])){
                        //Update
                        noFound = false;
                        userdata.remove(user);
                        ArrayList<String> list = new ArrayList<>();
                        list.add(winner[2]);
                        list.add(winner[0]);
                        list.add(winner[3]);
                        userdata.add(list);
                        break;
                    }
                    noFound = false;
                }
            }
            if(noFound == true){
                ArrayList<String> list = new ArrayList<>();
                list.add(winner[2]);
                list.add(winner[0]);
                list.add(winner[3]);
                userdata.add(list);
            }
        }

        for(File file: files){
            file.delete();
        }

        for(ArrayList content:userdata){
            File f = new File("hamburg/dbis/UserData/"+(content.get(0)));
            FileWriter fileWriter = null;
            try {
                fileWriter = new FileWriter(f);
                fileWriter.write(content.get(1) + "," + content.get(2));
                fileWriter.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("");

}}
