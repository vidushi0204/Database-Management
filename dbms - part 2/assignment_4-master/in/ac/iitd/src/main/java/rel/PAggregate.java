package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.ImmutableBitSet;

import convention.PConvention;

import java.util.*;

// Count, Min, Max, Sum, Avg
public class PAggregate extends Aggregate implements PRel {
    private final HashMap<List<Object>, List<Object[]>> groups;
    private List<Object[]> results;
    private int curr_index;
    public PAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
        assert getConvention() instanceof PConvention;
        this.groups = new HashMap<>();
        this.results = new ArrayList<>();
        this.curr_index = 0;
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new PAggregate(getCluster(), traitSet, hints, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public String toString() {
        return "PAggregate";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        logger.trace("Opening PAggregate");
        PRel inp = (PRel) input;
        if(!inp.open()) {
            logger.error("Error in opening PAggregate");
            return false;
        }
        while(inp.hasNext()) {
            Object[] row = inp.next();
            List<Object> key = new ArrayList<>();
            for(int i = 0; i < groupSet.cardinality(); i++) {
                key.add(row[i]);
            }
            if(!groups.containsKey(key)) {
                groups.put(key, new ArrayList<>());
            }
            groups.get(key).add(row);
        }
        AggregateGroups();
        return true;

    }
    private void AggregateGroups(){
        for(HashMap.Entry<List<Object>, List<Object[]>> entry : groups.entrySet()) {
            List<Object> key = entry.getKey();
            List<Object[]> rows = entry.getValue();
            int resultsize = aggCalls.size() + key.size();
            Object[] result = new Object[resultsize];
            for(int i = 0; i < key.size(); i++) {
                result[i] = key.get(i);
            }
            for(int i = 0; i < aggCalls.size(); i++) {
                AggregateCall aggCall = aggCalls.get(i);
                int j = i+key.size();
                switch(aggCall.getAggregation().getName().toUpperCase()) {
                    case "COUNT":
                        result[j] = rows.size();
                        break;
                    case "MIN":
                        result[j] = calc_min(rows,aggCall);
                        break;
                    case "MAX":
                        result[j] = calc_max(rows,aggCall);
                        break;
                    case "SUM":
                        result[j] = calc_sum(rows,aggCall);
                        break;
                    case "AVG":
                        result[j] = calc_avg(rows,aggCall);
                        break;
                    case "DISTINCT_COUNT":
                        result[j] = calc_count(rows,aggCall);
                        break;
                    default:
                        logger.error("Invalid Aggregation Function");
                        break;
                }
            }
            results.add(result);
        }
    }
    private Object calc_count(List<Object[]> rows, AggregateCall aggCall) {
        int index = aggCall.getArgList().get(0);
        return rows.stream().map(row->row[index]).filter(obj->obj!=null).distinct().count();

    }
    private Object calc_sum(List<Object[]> rows, AggregateCall aggCall) {
        int index = aggCall.getArgList().get(0);
        return rows.stream().map(row->row[index]).filter(obj->obj!=null).mapToDouble(obj->((Number)obj).doubleValue()).sum();
    }
    private Object calc_min(List<Object[]> rows, AggregateCall aggCall) {
        int index = aggCall.getArgList().get(0);
        return rows.stream().map(row->(Comparable)row[index]).filter(obj->obj!=null).min((obj1,obj2)->obj1.compareTo(obj2)).orElse(null);
    }
    private Object calc_max(List<Object[]> rows, AggregateCall aggCall) {
        int index = aggCall.getArgList().get(0);
        return rows.stream().map(row->(Comparable)row[index]).filter(obj->obj!=null).max((obj1,obj2)->obj1.compareTo(obj2)).orElse(null);
    }
    private Object calc_avg(List<Object[]> rows, AggregateCall aggCall) {
        int index = aggCall.getArgList().get(0);
        return rows.stream().map(row->row[index]).filter(obj->obj!=null).mapToDouble(obj->((Number)obj).doubleValue()).average().orElse(0);
    }
    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PAggregate");
        groups.clear();
        results.clear();
        curr_index=0;
        ((PRel)input).close();
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PAggregate has next");
        if(curr_index < results.size()) {
            return true;
        }
        return false;
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PAggregate");
        if(hasNext()) {
            return results.get(curr_index++);
        }
        return null;
    }

}