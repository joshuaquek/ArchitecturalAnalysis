package aa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.*;

/**
 * Some example code on how to use the MapReduce Framework
 */
public class Game2 implements Mapper, Reducer
{

    /**
     * A very simple mapper - returns the size of the data given to it
     * @param list the data shard for this mapper
     * @return HashMap: key = "Size", value = number of entries given to this mapper
     */
    @Override
    public HashMap map(List list)
    {
        HashMap <String, String> map = new HashMap<String, String>(); //map to be returned

        for (Object o: list)
        {
            String s = (String)o;
            String[] sArray = s.split(",");

            //data looks like: cheetah, 5.
            //puts "cheetah" as key, "5" as value.
            map.put(sArray[0], sArray[1]);
        }

        return map;
    }

    /**
     * A very simple reducer which just counts the number of mappers which passed data into this key.
     * Since each mapper returns at most one data element for a key, and all of those are passed as a list, the
     * size of the list is the number of mappers which generated data.
     *
     * In reality you'd want to do something with the data given by the mappers
     *  (sum them, concatenate them, search through them, etc)
     *
     * @param key the key given to this reducer to work on; only one key is given, all values for that key are given
     * @param data a list of all values for this key
     * @return key = given key ("Size"), value = total size of the data set
     */
    @Override
    public HashMap reduce(Object key, List data)
    {
        HashMap<Object, Integer> result = new HashMap<Object, Integer>(1);
        int value = 0;

        /* SUM */
        for (Object o : data)
        {
            String s = (String)o;
            Integer integer = Integer.parseInt(s.replaceAll("\\s",""));
            value += integer;
        }

        /* Count of number of entries for this key*/
//        value = data.size();

        //// output result
        result.put(key, value);
        return result;
    }
    /**
     * This shows how to read some data (currently one file as a command line argument)
     * Then launches the mapReduce framework
     * Prints the time taken to run
     * Outputs all the data returned from the Reducers
     * @param args a file
     */
    public static void main(String[] args)
    {

        //----------- DUMMY DATA SET -------
        List<String> data = new ArrayList<String>();
        data.add("Tiger, 5"); data.add("Tiger, 7"); data.add("Lion, 3");

        System.out.println("\n\n--------INPUT------------");
        for(String i : data){
            System.out.println(i);
        }
     
        

        System.out.println("\n\n------START MapReduce-------");
        Game2 mapper = new Game2(); //create mapper object
        Game2 reducer = new Game2(); //create reducer object

        HashMap<Object, List> results = null;

        System.gc(); //good to do garbage collection before timing anything
        long s = System.currentTimeMillis(); //ready, set, GO!

        //Here we start the map reduce job on ~3 shards, and ask for verbose output
        try { results = MapReduce.mapReduce(mapper, reducer, data, 3, true); }

        catch (InterruptedException e) //this shouldn't happen
        {
            System.out.println("Something unexpected happened");
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    

        System.out.println("\n\n--------RESULTS------------");
        Set<Object> resultKeys = results.keySet( ) ;
        for( Object i : resultKeys ){
            System.out.println( "Key: " + (String)i + "\tValue: " + results.get((String)i)  ) ;
        }     // Prints out all out all values.
        System.out.println("\n\n");
        
        long e = System.currentTimeMillis(); //And the winning time is....

        System.out.println("Clock time elapsed: " + (e - s) + " ms");

    }



}
