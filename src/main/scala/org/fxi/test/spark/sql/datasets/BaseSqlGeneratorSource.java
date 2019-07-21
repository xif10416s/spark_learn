package org.fxi.test.spark.sql.datasets;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

/**
 * Created by xifei on 16-10-14.
 */
public class BaseSqlGeneratorSource {
    public Object generate(Object[] references) {
        return new GeneratedIterator(references);
    }

    final class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {
        private Object[] references;
        private boolean agg_initAgg;
        private boolean agg_bufIsNull;
        private long agg_bufValue;
        private scala.collection.Iterator inputadapter_input;
        private scala.collection.Iterator inputadapter_input1;
        private org.apache.spark.sql.execution.metric.SQLMetric filter_numOutputRows;
        private UnsafeRow filter_result;
        private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder filter_holder;
        private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter filter_rowWriter;
        private scala.collection.Iterator inputadapter_input2;
        private org.apache.spark.sql.execution.metric.SQLMetric filter_numOutputRows1;
        private UnsafeRow filter_result1;
        private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder filter_holder1;
        private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter filter_rowWriter1;
        private org.apache.spark.sql.execution.metric.SQLMetric filter_numOutputRows2;
        private UnsafeRow filter_result2;
        private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder filter_holder2;
        private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter filter_rowWriter2;
        private org.apache.spark.sql.execution.metric.SQLMetric agg_numOutputRows;
        private org.apache.spark.sql.execution.metric.SQLMetric agg_aggTime;
        private UnsafeRow agg_result;
        private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder agg_holder;
        private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter agg_rowWriter;

        public GeneratedIterator(Object[] references) {
            this.references = references;
        }

        public void init(int index, scala.collection.Iterator inputs[]) {
            partitionIndex = index;
            agg_initAgg = false;

            inputadapter_input = inputs[0];
            inputadapter_input1 = inputs[0];
            this.filter_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[0];
            filter_result = new UnsafeRow(2);
            this.filter_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(filter_result, 32);
            this.filter_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(filter_holder, 2);
            inputadapter_input2 = inputs[0];
            this.filter_numOutputRows1 = (org.apache.spark.sql.execution.metric.SQLMetric) references[1];
            filter_result1 = new UnsafeRow(2);
            this.filter_holder1 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(filter_result1, 32);
            this.filter_rowWriter1 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(filter_holder1, 2);
            this.filter_numOutputRows2 = (org.apache.spark.sql.execution.metric.SQLMetric) references[2];
            filter_result2 = new UnsafeRow(2);
            this.filter_holder2 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(filter_result2, 32);
            this.filter_rowWriter2 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(filter_holder2, 2);
            this.agg_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[3];
            this.agg_aggTime = (org.apache.spark.sql.execution.metric.SQLMetric) references[4];
            agg_result = new UnsafeRow(1);
            this.agg_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(agg_result, 0);
            this.agg_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(agg_holder, 1);
        }

        private void agg_doAggregateWithoutKey() throws java.io.IOException {
// initialize aggregation buffer
            agg_bufIsNull = false;
            agg_bufValue = 0L;

            while (inputadapter_input.hasNext()) {
                InternalRow inputadapter_row = (InternalRow) inputadapter_input.next();
                long agg_value8 = inputadapter_row.getLong(1);

                boolean filter_isNull14 = false;

                boolean filter_isNull15 = false;

                long filter_value15 = -1L;
                filter_value15 = agg_value8 + 1L;

                boolean filter_value14 = false;
                filter_value14 = filter_value15 > 20L;
                if (!filter_value14) continue;

                filter_numOutputRows2.add(1);

// do aggregate
// common sub-expressions

// evaluate aggregate function
                boolean agg_isNull9 = false;

                long agg_value9 = -1L;
                agg_value9 = agg_bufValue + 1L;
// update aggregation buffer
                agg_bufIsNull = false;
                agg_bufValue = agg_value9;
                if (shouldStop()) return;
            }

        }

        protected void processNext() throws java.io.IOException {
            while (!agg_initAgg) {
                agg_initAgg = true;
                long agg_beforeAgg = System.nanoTime();
                agg_doAggregateWithoutKey();
                agg_aggTime.add((System.nanoTime() - agg_beforeAgg) / 1000000);

// output the result

                agg_numOutputRows.add(1);
                agg_rowWriter.zeroOutNullBytes();

                if (agg_bufIsNull) {
                    agg_rowWriter.setNullAt(0);
                } else {
                    agg_rowWriter.write(0, agg_bufValue);
                }
                append(agg_result);
            }
        }
    }
}
