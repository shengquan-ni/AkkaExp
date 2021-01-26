package Engine.Operators.Common.FlatMap;

import Engine.Common.AmberTag.LayerTag;
import Engine.Common.AmberTuple.Tuple;
import Engine.Common.TupleProcessor;
import scala.Function1;
import scala.collection.Iterator;


public class FlatMapTupleProcessor implements TupleProcessor {

    private Iterator<Tuple> tupleIterator = null;
    private boolean nextFlag = false;
    private Function1<Tuple, Iterator<Tuple>> flatMapFunc;

    FlatMapTupleProcessor(Function1<Tuple, Iterator<Tuple>> flatMapFunc){
        this.flatMapFunc = flatMapFunc;
    }

    @Override
    public void accept(Tuple tuple) throws Exception {
        this.tupleIterator = flatMapFunc.apply(tuple);
        this.nextFlag = tupleIterator.toIterator().hasNext();
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
        Tuple next = tupleIterator.next();
        this.nextFlag = tupleIterator.hasNext();
        return next;
    }

    @Override
    public void dispose() throws Exception {
    }
}
