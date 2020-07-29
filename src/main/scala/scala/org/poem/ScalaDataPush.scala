package scala.org.poem

import com.alibaba.fastjson.JSON
import org.poem.config.DatabaseContainer
import org.poem.vo.ExecTaskDetailPlanVO
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.util.StringUtils
import scala.collection.JavaConverters

object ScalaDataPush {

  def main(args: Array[String]): Unit = {
    val par: String = args.apply(0)
    val execTaskDetailPlanVO: ExecTaskDetailPlanVO = JSON.parseObject(par, classOf[ExecTaskDetailPlanVO])
    val targetTemplate = DatabaseContainer.getTargetJdbc(execTaskDetailPlanVO)
    val sourceTemplate = DatabaseContainer.getSourceJdbc(execTaskDetailPlanVO)

    val aroundSql = execTaskDetailPlanVO.getBeforeSql
    val results = sourceTemplate.queryForList(aroundSql)
    val scalaResults = JavaConverters.asScalaIteratorConverter(results.iterator()).asScala.toArray
    insertDataResultToDatabases(scalaResults, sourceTemplate,targetTemplate, execTaskDetailPlanVO)
  }

  /**
   * insert into data to databases
   *
   * @param data
   * @param sourceTemplate
   * @param targetTemplate
   */
  def insertDataResultToDatabases(data: Array[Map[String, Object]],
                                sourceTemplate: JdbcTemplate,
                                targetTemplate: JdbcTemplate,
                                execTaskDetailPlanVO: ExecTaskDetailPlanVO): Unit = {
    val aroundSql = execTaskDetailPlanVO.getAroundSql
    val sqls = data.map(
      datum => {
        var sql = String.copyValueOf(aroundSql.toCharArray)
        for (s <- datum.keySet) {
          sql = sql.replaceAll("@" + s, datum.get(s).toString)
        }
        sql
      }.filter(d => {
        !StringUtils.isEmpty(d)
      })
    )

    for (elem <- sqls) {
      targetTemplate.update(elem)
    }
  }
}
