package Engine.Operators.Common.Filter;

import Engine.Common.AmberTag.LayerTag;
import Engine.Common.AmberTuple.Tuple;
import Engine.Common.TupleProcessor;
import scala.Function1;


public class FilterTupleProcessor implements TupleProcessor {

    private Tuple tuple = null;
    private boolean nextFlag = false;
    private Function1<Tuple, Boolean> filterFunc;

    FilterTupleProcessor(Function1<Tuple, Boolean> filterFunc){
        this.filterFunc = filterFunc;
    }

    @Override
    public void accept(Tuple tuple) throws Exception {
        this.nextFlag = filterFunc.apply(tuple);
        if (this.nextFlag) {
            this.tuple = tuple;
        } else {
            this.tuple = null;
        }
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
