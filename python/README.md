# README

## 功能说明
`SimModel.py` 以 `fake_user`, `normal_user` 作为训练集，将训练的5个模型("NaiveBayesian","LogisticRegression","KNeighborsClassifier","RandomForest","OneClassSVM","SVC")存在了models里面，预测的时候只需调用相应模型即可进行预测。
输出的结果是 `dictionary` ，`key` 是模型的名字，`value` 是模型对应的准确率，准确率计算是按窜卡用户计算的

## 使用说明
只要将对应的 `fake_use` r, `normal_user` 数据路径写入代码，运行即可获取相应的训练模型，存入 `models` 文件。调用 `model_reuse` 函数即可预测未知用户窜卡概率。