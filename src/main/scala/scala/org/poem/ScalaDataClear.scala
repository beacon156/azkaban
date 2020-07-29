package scala.org.poem

import com.alibaba.fastjson.JSON
import org.poem.config.DatabaseContainer
import org.poem.vo.ExecTaskDetailPlanVO

object ScalaDataClear {


  def main(args: Array[String]): Unit = {
    val par: String = args.apply(0)
    val execTaskDetailPlanVO: ExecTaskDetailPlanVO = JSON.parseObject(par, classOf[ExecTaskDetailPlanVO]);
    val targetTemplate = DatabaseContainer.getTargetJdbc(execTaskDetailPlanVO)
    val aroundSql = execTaskDetailPlanVO.getAroundSql
    targetTemplate.update(aroundSql)
  }

}
