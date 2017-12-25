package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator _child;
    private int _afield;
    private int _gfield;
    private Aggregator.Op _aop;
    private OpIterator _iterator; 
    
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
    	_child = child;
    	_afield = afield;
    	_gfield = gfield;
    	_aop = aop;
    	_iterator = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
    	return _gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
    	return _child.getTupleDesc().getFieldName(_gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
    	return _afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
		return _child.getTupleDesc().getFieldName(_afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
		return _aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
    	super.open();
    	_child.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
    	if(_iterator==null) {
    		if(!_child.hasNext())
    			return null;
    		Type _gfieldType = _gfield==-1?null:_child.getTupleDesc().getFieldType(_gfield);
    		if(_child.getTupleDesc().getFieldType(_afield).equals(Type.INT_TYPE)) {
        		Aggregator agg = new IntegerAggregator(_gfield,_gfieldType,_afield,_aop);
        		while(_child.hasNext())
        			agg.mergeTupleIntoGroup(_child.next());
        		_iterator = agg.iterator();
        	}
    		if(_child.getTupleDesc().getFieldType(_afield).equals(Type.STRING_TYPE)) {
        		Aggregator agg = new StringAggregator(_gfield,_gfieldType,_afield,_aop);
        		while(_child.hasNext())
        			agg.mergeTupleIntoGroup(_child.next());
        		_iterator = agg.iterator();
        	}
    		_iterator.open();
    	}
		
    	if(_iterator.hasNext())
    		return _iterator.next();
    	else
    		return null;
    	
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
    	//_child.rewind();
    	close();
    	open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
    	TupleDesc td;
    	
        if(_gfield==-1){
        	td=new TupleDesc(new Type[] {_child.getTupleDesc().getFieldType(_afield)});
        }
        else
        	td=new TupleDesc(new Type[] {_child.getTupleDesc().getFieldType(_gfield), _child.getTupleDesc().getFieldType(_afield)});
        return td;
    }

    public void close() {
	// some code goes here
    	_child.close();
    	_iterator=null;
    	super.close();
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
    	return new OpIterator[]{_child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
    	_child = children[0];
    }
    
}
