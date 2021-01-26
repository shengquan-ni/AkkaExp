package Engine.Operators.Common.Map;

import Engine.Common.AmberTag.LayerTag;
import Engine.Common.AmberTuple.Tuple;
import Engine.Common.TupleProcessor;
import scala.Function1;


public class MapTupleProcessor implements TupleProcessor {

    private Tuple tuple = null;
    private boolean nextFlag = false;
    private Function1<Tuple, Tuple> mapFunc;

    MapTupleProcessor(Function1<Tuple, Tuple> mapFunc){
        this.mapFunc = mapFunc;
    }

    @Override
    public void accept(Tuple tuple) throws Exception {
        nextFlag = true;
        this.tuple = mapFunc.apply(tuple);
    }

    @Override
    public void onUpstreamChanged(LayerTag from) {
    }

    @Override
    public void onUpstreamExhausted(LayerTag from) {
    }

    @Override
    public void noMore() {
    }

    @Override
    public void initialize() throws Exception {
    }

    @Override
    public boolean hasNext() throws Exception {
        return nextFlag;
    }

    @Override
    public Tuple next() throws Exception {
        nextFlag = false;
        return tuple;
    }

    @Override
    public void dispose() throws Exception {
    }
}
