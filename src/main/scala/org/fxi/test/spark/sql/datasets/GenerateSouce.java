//package org.fxi.test.spark.sql.datasets;
//
//import org.apache.spark.sql.catalyst.InternalRow;
//import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
//import org.apache.spark.unsafe.types.UTF8String;
//
///**
// * Created by xifei on 16-9-30.
// */
//public class  GenerateSouce {
//
//    /* 001 */ public Object generate(Object[] references) {
///* 002 */   return new GeneratedIterator(references);
///* 003 */ }
//    /* 004 */
///* 005 */ final class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {
//        /* 006 */   private Object[] references;
//        /* 007 */   private org.apache.spark.sql.execution.metric.SQLMetric scan_numOutputRows;
//        /* 008 */   private scala.collection.Iterator scan_input;
//        /* 009 */   private org.apache.spark.sql.execution.metric.SQLMetric filter_numOutputRows;
//        /* 010 */   private UnsafeRow filter_result;
//        /* 011 */   private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder filter_holder;
//        /* 012 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter filter_rowWriter;
//        /* 013 */   private UnsafeRow project_result;
//        /* 014 */   private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder project_holder;
//        /* 015 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter project_rowWriter;
//        /* 016 */   private org.apache.spark.sql.execution.metric.SQLMetric scan_numOutputRows1;
//        /* 017 */   private scala.collection.Iterator scan_input1;
//        /* 018 */   private org.apache.spark.sql.execution.metric.SQLMetric filter_numOutputRows1;
//        /* 019 */   private UnsafeRow filter_result1;
//        /* 020 */   private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder filter_holder1;
//        /* 021 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter filter_rowWriter1;
//        /* 022 */   private UnsafeRow project_result1;
//        /* 023 */   private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder project_holder1;
//        /* 024 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter project_rowWriter1;
//        /* 025 */   private org.apache.spark.sql.execution.metric.SQLMetric scan_numOutputRows2;
//        /* 026 */   private scala.collection.Iterator scan_input2;
//        /* 027 */   private org.apache.spark.sql.execution.metric.SQLMetric filter_numOutputRows2;
//        /* 028 */   private UnsafeRow filter_result2;
//        /* 029 */   private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder filter_holder2;
//        /* 030 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter filter_rowWriter2;
//        /* 031 */   private UnsafeRow project_result2;
//        /* 032 */   private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder project_holder2;
//        /* 033 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter project_rowWriter2;
//        /* 034 */   private org.apache.spark.sql.execution.metric.SQLMetric scan_numOutputRows3;
//        /* 035 */   private scala.collection.Iterator scan_input3;
//        /* 036 */   private org.apache.spark.sql.execution.metric.SQLMetric filter_numOutputRows3;
//        /* 037 */   private UnsafeRow filter_result3;
//        /* 038 */   private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder filter_holder3;
//        /* 039 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter filter_rowWriter3;
//        /* 040 */   private UnsafeRow project_result3;
//        /* 041 */   private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder project_holder3;
//        /* 042 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter project_rowWriter3;
//        /* 043 */   private org.apache.spark.sql.execution.metric.SQLMetric scan_numOutputRows4;
//        /* 044 */   private scala.collection.Iterator scan_input4;
//        /* 045 */   private org.apache.spark.sql.execution.metric.SQLMetric filter_numOutputRows4;
//        /* 046 */   private UnsafeRow filter_result4;
//        /* 047 */   private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder filter_holder4;
//        /* 048 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter filter_rowWriter4;
//        /* 049 */   private UnsafeRow project_result4;
//        /* 050 */   private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder project_holder4;
//        /* 051 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter project_rowWriter4;
//        /* 052 */   private org.apache.spark.sql.execution.metric.SQLMetric scan_numOutputRows5;
//        /* 053 */   private scala.collection.Iterator scan_input5;
//        /* 054 */   private org.apache.spark.sql.execution.metric.SQLMetric filter_numOutputRows5;
//        /* 055 */   private UnsafeRow filter_result5;
//        /* 056 */   private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder filter_holder5;
//        /* 057 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter filter_rowWriter5;
//        /* 058 */   private UnsafeRow project_result5;
//        /* 059 */   private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder project_holder5;
//        /* 060 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter project_rowWriter5;
//        /* 061 */   private org.apache.spark.sql.execution.metric.SQLMetric scan_numOutputRows6;
//        /* 062 */   private scala.collection.Iterator scan_input6;
//        /* 063 */   private org.apache.spark.sql.execution.metric.SQLMetric filter_numOutputRows6;
//        /* 064 */   private UnsafeRow filter_result6;
//        /* 065 */   private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder filter_holder6;
//        /* 066 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter filter_rowWriter6;
//        /* 067 */   private UnsafeRow project_result6;
//        /* 068 */   private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder project_holder6;
//        /* 069 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter project_rowWriter6;
//        /* 070 */
///* 071 */   public GeneratedIterator(Object[] references) {
///* 072 */     this.references = references;
///* 073 */   }
//        /* 074 */
///* 075 */   public void init(int index, scala.collection.Iterator inputs[]) {
///* 076 */     partitionIndex = index;
///* 077 */     this.scan_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[0];
///* 078 */     scan_input = inputs[0];
///* 079 */     this.filter_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[1];
///* 080 */     filter_result = new UnsafeRow(2);
///* 081 */     this.filter_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(filter_result, 32);
///* 082 */     this.filter_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(filter_holder, 2);
///* 083 */     project_result = new UnsafeRow(2);
///* 084 */     this.project_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(project_result, 32);
///* 085 */     this.project_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(project_holder, 2);
///* 086 */     this.scan_numOutputRows1 = (org.apache.spark.sql.execution.metric.SQLMetric) references[2];
///* 087 */     scan_input1 = inputs[0];
///* 088 */     this.filter_numOutputRows1 = (org.apache.spark.sql.execution.metric.SQLMetric) references[3];
///* 089 */     filter_result1 = new UnsafeRow(2);
///* 090 */     this.filter_holder1 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(filter_result1, 32);
///* 091 */     this.filter_rowWriter1 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(filter_holder1, 2);
///* 092 */     project_result1 = new UnsafeRow(2);
///* 093 */     this.project_holder1 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(project_result1, 32);
///* 094 */     this.project_rowWriter1 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(project_holder1, 2);
///* 095 */     this.scan_numOutputRows2 = (org.apache.spark.sql.execution.metric.SQLMetric) references[4];
///* 096 */     scan_input2 = inputs[0];
///* 097 */     this.filter_numOutputRows2 = (org.apache.spark.sql.execution.metric.SQLMetric) references[5];
///* 098 */     filter_result2 = new UnsafeRow(2);
///* 099 */     this.filter_holder2 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(filter_result2, 32);
///* 100 */     this.filter_rowWriter2 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(filter_holder2, 2);
///* 101 */     project_result2 = new UnsafeRow(2);
///* 102 */     this.project_holder2 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(project_result2, 32);
///* 103 */     this.project_rowWriter2 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(project_holder2, 2);
///* 104 */     this.scan_numOutputRows3 = (org.apache.spark.sql.execution.metric.SQLMetric) references[6];
///* 105 */     scan_input3 = inputs[0];
///* 106 */     this.filter_numOutputRows3 = (org.apache.spark.sql.execution.metric.SQLMetric) references[7];
///* 107 */     filter_result3 = new UnsafeRow(2);
///* 108 */     this.filter_holder3 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(filter_result3, 32);
///* 109 */     this.filter_rowWriter3 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(filter_holder3, 2);
///* 110 */     project_result3 = new UnsafeRow(2);
///* 111 */     this.project_holder3 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(project_result3, 32);
///* 112 */     this.project_rowWriter3 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(project_holder3, 2);
///* 113 */     this.scan_numOutputRows4 = (org.apache.spark.sql.execution.metric.SQLMetric) references[8];
///* 114 */     scan_input4 = inputs[0];
///* 115 */     this.filter_numOutputRows4 = (org.apache.spark.sql.execution.metric.SQLMetric) references[9];
///* 116 */     filter_result4 = new UnsafeRow(2);
///* 117 */     this.filter_holder4 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(filter_result4, 32);
///* 118 */     this.filter_rowWriter4 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(filter_holder4, 2);
///* 119 */     project_result4 = new UnsafeRow(2);
///* 120 */     this.project_holder4 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(project_result4, 32);
///* 121 */     this.project_rowWriter4 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(project_holder4, 2);
///* 122 */     this.scan_numOutputRows5 = (org.apache.spark.sql.execution.metric.SQLMetric) references[10];
///* 123 */     scan_input5 = inputs[0];
///* 124 */     this.filter_numOutputRows5 = (org.apache.spark.sql.execution.metric.SQLMetric) references[11];
///* 125 */     filter_result5 = new UnsafeRow(2);
///* 126 */     this.filter_holder5 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(filter_result5, 32);
///* 127 */     this.filter_rowWriter5 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(filter_holder5, 2);
///* 128 */     project_result5 = new UnsafeRow(2);
///* 129 */     this.project_holder5 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(project_result5, 32);
///* 130 */     this.project_rowWriter5 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(project_holder5, 2);
///* 131 */     this.scan_numOutputRows6 = (org.apache.spark.sql.execution.metric.SQLMetric) references[12];
///* 132 */     scan_input6 = inputs[0];
///* 133 */     this.filter_numOutputRows6 = (org.apache.spark.sql.execution.metric.SQLMetric) references[13];
///* 134 */     filter_result6 = new UnsafeRow(2);
///* 135 */     this.filter_holder6 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(filter_result6, 32);
///* 136 */     this.filter_rowWriter6 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(filter_holder6, 2);
///* 137 */     project_result6 = new UnsafeRow(2);
///* 138 */     this.project_holder6 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(project_result6, 32);
///* 139 */     this.project_rowWriter6 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(project_holder6, 2);
///* 140 */   }
//        /* 141 */
///* 142 */   protected void processNext() throws java.io.IOException {
///* 143 */     while (scan_input.hasNext()) {
///* 144 */       InternalRow scan_row = (InternalRow) scan_input.next();
///* 145 */       scan_numOutputRows.add(1);
///* 146 */       boolean scan_isNull2 = scan_row.isNullAt(0);
///* 147 */       long scan_value2 = scan_isNull2 ? -1L : (scan_row.getLong(0));
///* 148 */
///* 149 */       if (!(!(scan_isNull2))) continue;
///* 150 */
///* 151 */       boolean filter_isNull2 = false;
///* 152 */
///* 153 */       boolean filter_value2 = false;
///* 154 */       filter_value2 = scan_value2 > 19L;
///* 155 */       if (!filter_value2) continue;
///* 156 */
///* 157 */       filter_numOutputRows.add(1);
///* 158 */
///* 159 */       boolean scan_isNull3 = scan_row.isNullAt(1);
///* 160 */       UTF8String scan_value3 = scan_isNull3 ? null : (scan_row.getUTF8String(1));
///* 161 */       project_holder.reset();
///* 162 */
///* 163 */       project_rowWriter.zeroOutNullBytes();
///* 164 */
///* 165 */       project_rowWriter.write(0, scan_value2);
///* 166 */
///* 167 */       if (scan_isNull3) {
///* 168 */         project_rowWriter.setNullAt(1);
///* 169 */       } else {
///* 170 */         project_rowWriter.write(1, scan_value3);
///* 171 */       }
///* 172 */       project_result.setTotalSize(project_holder.totalSize());
///* 173 */       append(project_result);
///* 174 */       if (shouldStop()) return;
///* 175 */     }
///* 176 */   }
///* 177 */ }
//}
