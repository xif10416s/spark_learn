package org.fxi.test.spark.sql.catalyst

import org.apache.spark.sql.{Row}

/**
  * Created by xifei on 16-10-14.
  */
package object mock {
  type DataFrameMock = DataSetMock[Row]
}
