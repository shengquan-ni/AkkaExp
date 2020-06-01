package Engine.Operators.Count;

import Engine.Common.AmberTag.LayerTag;
import Engine.Common.AmberTuple.Tuple;
import Engine.Common.TupleProcessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class CountGlobalTupleProcessor implements TupleProcessor {

    private boolean nextFlag = false;
    private int counter = 0;

    @Override
    public void accept(Tuple tuple){
        counter += tuple.getInt(0);
    }

    @Override
    public void onUpstreamChanged(LayerTag from) {

    }

    @Override
    public void onUpstreamExhausted(LayerTag from) {

    }

    @Override
    public void noMore() {
        nextFlag = true;
    }

    @Override
    public void initialize() {

    }

    @Override
    public boolean hasNext() {
        return nextFlag;
    }

    @Override
    public Tuple next() {
        nextFlag = false;
        return Tuple.fromJavaList(Collections.singletonList(counter));
    }

    @Override
    public void dispose() {

    }

    @Override
    public Object getBuildHashTable() {
        return null;
    }

    @Override
    public void renewHashTable(Object hashTable) {

    }
}
