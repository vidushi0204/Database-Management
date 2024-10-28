package rel;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;

import java.util.Comparator;
import java.util.ArrayList;
// import java.util.Comparable;
import convention.PConvention;

public class PSort extends Sort implements PRel{
    private int curr_index;
    private final List<Object[]> sorted_rows;
    int sorted_rows_size;
    public PSort(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelNode child,
            RelCollation collation,
            RexNode offset,
            RexNode fetch
            ) {
        super(cluster, traits, hints, child, collation, offset, fetch);
        assert getConvention() instanceof PConvention;
        this.sorted_rows=new ArrayList<>();
        this.curr_index=0;
        this.sorted_rows_size=0;
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new PSort(getCluster(), traitSet, hints, input, collation, offset, fetch);
    }

    @Override
    public String toString() {
        return "PSort";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PSort");
        /* Write your code here */
        if(((PRel)input).open()){
            while(((PRel)input).hasNext()){
                sorted_rows.add(((PRel)input).next());
                sorted_rows_size++;
            }
            ((PRel)input).close();
            Collections.sort(sorted_rows,give_comparer());
            return true;
        }
        return false;
    }
    private Comparator<Object[]> give_comparer(){
        return new Comparator<Object[]>(){
            @Override
            public int compare(Object[] o1, Object[] o2){
                List<RelFieldCollation> collation_list=collation.getFieldCollations();
                for(int i=0;i<collation_list.size();i++){
                    int index=collation_list.get(i).getFieldIndex();
                    Comparable obj1=(Comparable)o1[index];
                    Comparable obj2=(Comparable)o2[index];
                    int comp = obj1.compareTo(obj2);
                    if(comp!=0){
                        return collation_list.get(i).getDirection().isDescending()?-comp:comp;
                    }
                }
                return 0;
            }
        };
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PSort");
        sorted_rows.clear();
        sorted_rows_size=0;
        curr_index=0;
        /* Write your code here */
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PSort has next");
        if(curr_index<sorted_rows_size)
            return true;
        return false;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PSort");
        if(hasNext()){
            return sorted_rows.get(curr_index++);
        }
        /* Write your code here */
        return null;
    }

}
