package Engine.Operators.HashJoin;

import Engine.Common.AmberTag.LayerTag;
import Engine.Common.AmberTuple.AmberTuple;
import Engine.Common.AmberTuple.Tuple;
import Engine.Common.TupleProcessor;
import com.google.inject.internal.cglib.core.$ClassNameReader;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class HashJoinTupleProcessor<K> implements TupleProcessor {

    private LayerTag innerTableIdentifier;
    private int innerTableIndex;
    private int outerTableIndex;
    private boolean isCurrentTableInner = false;
    private boolean isInnerTableFinished = false;
    private HashMap<K, ArrayList<Object[]>> innerTableHashMap = null;
    private Iterator<Object[]> currentEntry = null;
    private Object[] currentTuple = null;

    private boolean notInitializedInner = true;
    private boolean notInitializedOuter = true;

    HashJoinTupleProcessor(LayerTag innerTableIdentifier, int innerTableIndex, int outerTableIndex){
        this.innerTableIdentifier = innerTableIdentifier;
        this.innerTableIndex = innerTableIndex;
        this.outerTableIndex = outerTableIndex;
    }


    @SuppressWarnings("unchecked")
    @Override
    public void accept(Tuple tuple) {
        if(isCurrentTableInner){
            K key = (K)tuple.get(innerTableIndex);
            if(!innerTableHashMap.containsKey(key)) {
                innerTableHashMap.put(key,new ArrayList<>());
            }
            innerTableHashMap.get(key).add(ArrayUtils.remove(tuple.toArray(),innerTableIndex));

            // Below is custom code to build fake data
            if(outerTableIndex == 1) {
                for(int i =0; i<10; i++) {
                    innerTableHashMap.get(key).add(ArrayUtils.remove(tuple.toArray(),innerTableIndex));
                }
            }

            if(notInitializedInner && outerTableIndex == 1) {
                for(int i=13; i<1000000; i++) {
                    K key1 = (K)Integer.toString(i);
                    innerTableHashMap.put(key1,new ArrayList<>());
                    innerTableHashMap.get(key1).add(ArrayUtils.remove(tuple.toArray(),innerTableIndex));
                }

                notInitializedInner = false;
            }
        }else{
            if(!isInnerTableFinished) {
                throw new AssertionError("Probe table came before build table");
            }else{
                if(notInitializedOuter) {
                    System.out.println("Inner length " + innerTableHashMap.size());
                    notInitializedOuter = false;
                }
                K key = (K)tuple.get(outerTableIndex);
                if(innerTableHashMap.containsKey(key)) {
                    currentEntry = innerTableHashMap.get(key).iterator();
                    currentTuple = tuple.toArray();
                }
            }
        }
    }

    @Override
    public void onUpstreamChanged(LayerTag from) {
        isCurrentTableInner = innerTableIdentifier.equals(from);
    }

    @Override
    public void onUpstreamExhausted(LayerTag from) {
        isInnerTableFinished = innerTableIdentifier.equals(from);
    }

    @Override
    public void noMore() {

    }

    @Override
    public void initialize() { innerTableHashMap = new HashMap<>(); }

    @Override
    public boolean hasNext() {
        return currentEntry != null && currentEntry.hasNext();
    }

    @Override
    public Tuple next() {
        return new AmberTuple(ArrayUtils.addAll(currentTuple,currentEntry.next()));
    }

    @Override
    public void dispose() {
        // innerTableHashMap = null;
        currentEntry = null;
        currentTuple = null;
    }

    @Override
    public ArrayList<Object> getBuildHashTable() {
        ArrayList<Object> sendingMap = new ArrayList<>();
        int count = 1;
        HashMap<K, ArrayList<Object[]>> curr = new HashMap<>();
        for(Map.Entry<K, ArrayList<Object[]>> entry: innerTableHashMap.entrySet()) {
            curr.put(entry.getKey(),entry.getValue());
            if(count % 4000 == 0) {
                sendingMap.add(curr);
                curr = new HashMap<>();
            }
            count++;
        }
        if(!curr.isEmpty()) {
            sendingMap.add(curr);
        }

        return sendingMap;
    }

    @Override
    public void renewHashTable(Object hashTable) {
        HashMap<K, ArrayList<Object[]>> newHashMap = (HashMap<K, ArrayList<Object[]>>)hashTable;
        for(Map.Entry<K, ArrayList<Object[]>> entry: newHashMap.entrySet()) {
            if(!innerTableHashMap.containsKey(entry.getKey())) {
                innerTableHashMap.put(entry.getKey(),new ArrayList<>());
            }
            innerTableHashMap.get(entry.getKey()).addAll(entry.getValue());
        }
        // System.out.println("Inner length for new table" + innerTableHashMap.size());
    }
}