# README

## 功能说明
`normal_user.scala` 是用来获取虚假用户公司以外公司某一天所有物联网卡相关的数据
`fake_user.scala` 是用来获取虚假用户公司某一天所有物联网卡相关的数据
`read_file.scala` 是将正常用户和虚假用户分别获得的数据进行整合，分别生成七天的数据为后面模型的训练做准备
`cycle_date.scala` 因为平台时间和国内时间有时差，用作转换时间差。

## 使用说明
将对应代码生成jar包上传，
`fake_user.scala` 使用以下命令运行变可获得相应结果相应结果  nohup /opt/spark2/bin/spark-submit --class className /xxxxxxxx/IoT.jar Date

`read_file.scala` 使用以下命令运行变可获得相应结果相应结果  nohup /opt/spark2/bin/spark-submit --class className /xxxxxxxx/IoT.jar
