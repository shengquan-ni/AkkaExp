package Engine.Operators.HashJoin;

import Engine.Common.AmberTag.LayerTag;
import Engine.Common.AmberTuple.AmberTuple;
import Engine.Common.AmberTuple.Tuple;
import Engine.Common.TupleProcessor;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class HashJoinTupleProcessor<K> implements TupleProcessor {

    private LayerTag innerTableIdentifier;
    private int innerTableIndex;
    private int outerTableIndex;
    private boolean isCurrentTableInner = false;
    private boolean isInnerTableFinished = false;
    private HashMap<K, ArrayList<Tuple>> innerTableHashMap = null;
    private Iterator<Tuple> currentEntry = null;
    private Object[] currentTuple = null;

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
            innerTableHashMap.get(key).add(tuple);
        }else{
            if(!isInnerTableFinished) {
                throw new AssertionError();
            }else{
                K key = (K)tuple.get(outerTableIndex);
                if(innerTableHashMap.containsKey(key)) {
                    currentEntry = innerTableHashMap.get(key).iterator();
                    currentTuple = ArrayUtils.remove(tuple.toArray(),innerTableIndex);
                }
            }
        }
    }

    @Override
    public void onUpstreamChanged(LayerTag from) {
        isCurrentTableInner = innerTableIdentifier == from;
    }

    @Override
    public void onUpstreamExhausted(LayerTag from) {
        isInnerTableFinished = innerTableIdentifier == from;
    }

    @Override
    public void noMore() {

    }

    @Override
    public void initialize() {
        innerTableHashMap = new HashMap<>();
    }

    @Override
    public boolean hasNext() {
        return currentEntry != null && currentEntry.hasNext();
    }

    @Override
    public Tuple next() {
        return new AmberTuple(ArrayUtils.addAll(currentTuple,currentEntry.next().toArray()));
    }

    @Override
    public void dispose() {

    }
}