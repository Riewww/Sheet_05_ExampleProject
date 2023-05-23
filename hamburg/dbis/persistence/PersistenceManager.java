package hamburg.dbis.persistence;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;

public class PersistenceManager {

    static final private PersistenceManager _manager;

    // TODO Add class variables if necessary
    public LinkedList<ArrayList<Object>> buffer = new LinkedList<ArrayList<Object>>();
    public LinkedList<Integer> lsns = new LinkedList<Integer>();
    public ArrayList<Integer> transaction_id = new ArrayList<>();


    static {
        try {
            _manager = new PersistenceManager();
        } catch (Throwable e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private PersistenceManager() {
        // TODO Get the last used transaction id from the log (if present) at startup
        // TODO Initialize class variables if necessary

        try {
            String line;
            FileReader fileReader = new FileReader("hamburg/dbis/persistence/log.txt");
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while((line=bufferedReader.readLine()) != null){
                transaction_id.add(Integer.parseInt(line.split("\t")[0]));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static public PersistenceManager getInstance() {
        return _manager;
    }

    public synchronized int beginTransaction() {
        // TODO return a valid transaction id to the client
        Integer taid;
        if(transaction_id.size()==0){
            taid = 1;
        }
        else{
            taid = transaction_id.get(transaction_id.size()-1)+1;
        }
        transaction_id.add(taid);
        return taid;
    }

    public void commit(int taid) {
        // TODO handle commits
        write(taid,0,"EOT");
        transaction_id.remove(transaction_id.indexOf(taid));
    }

    public void write(int taid, int pageid, String data) {
        // TODO handle writes of Transaction taid on page pageid with data
        Integer lsn;
        if(lsns.size() == 0){
            lsn = 1;
        }
        else{
            lsn = lsns.pop()+1;
        }
        lsns.add(lsn);
        try {
        FileWriter logfile = new FileWriter("hamburg/dbis/persistence/log.txt",true);
        if(data == "EOT"){
            logfile.write(lsn + "\t" + taid + "\t" + data + "\n");
            logfile.close();
        }
        else{
            logfile.write(lsn + "\t" + taid + "\t" + pageid + "\t" + data + "\n");
            logfile.close();
        }
        ArrayList<Object> list = new ArrayList<>();
        list.add(lsn);
        list.add(taid);
        list.add(pageid);
        list.add(data);
        buffer.add(list);

        ArrayList<ArrayList> writedata = new ArrayList<>();
        if(buffer.size()>5){
            for(int i = 0; i < buffer.size();i++){
                if(transaction_id.contains(buffer.get(i).get(0))){
                    writedata.add(buffer.get(i));
                    buffer.remove(i);
                }
            }
        }
        for(ArrayList content:writedata){
            File f = new File("hamburg/dbis/UserData/"+Integer.toString((Integer) content.get(2)));
            FileWriter fileWriter = new FileWriter(f);
            fileWriter.write(content.get(0) + "," + content.get(3));
            fileWriter.close();
        }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
