#coding:utf-8
import os
import sys
import numpy as np
from sklearn.naive_bayes import GaussianNB
from sklearn.model_selection import train_test_split
from sklearn.model_selection import KFold
from sklearn import svm
from itertools import islice
from sklearn.linear_model import LogisticRegression
from sklearn .ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.preprocessing import StandardScaler
import random
from sklearn.externals import joblib

os.getcwd()
reload(sys)
sys.setdefaultencoding( "utf-8" )
class SimModel():
    fakeFilePath = "XXXXXXXXXX"
    normalFilePath = "XXXXXXXXXXX"
    testFilePath = "XXXXXXXXXXXX"
    def read_file(self, filePath, flag):
        dataset = []
        labels = []
        try:
            fd = open(filePath)
        except:
            print "this file does not exit"
        num = 0
        for line in islice(fd, 1, None):
            line = line.strip().split(',')
            line_data = []
            num += 1
            try:
                for i in range(4, len(line)):
                    line_data.append(int(line[i]))
            except:
                continue
            dataset.append(line_data)
            labels.append(flag)
            if num > 50000:
                break
        print "read_data"
        return dataset, labels

    def shuffle_data(self, data, label):
        total = zip(data, label)
        random.shuffle(total)
        data = [e[0] for e in total]
        label = [e[1] for e in total]
        print "shuffle_data"
        return data, label

    def stepOne_model(self, X, Y, X_test):
        normal_data = []
        normal_label = []
        clf1 = GaussianNB()
        clf1.fit(X, Y)
        y_pred = clf1.predict(X_test)
        for i in [index for index in range(len(y_pred)) if y_pred[index] == 1]:
            normal_data.append(X_test[i])
            normal_label.append(y_pred[i])
        return normal_data, normal_label

    def read_file_train(self, fakeFilePath, normalFilePath):
        fake_data, fake_label = self.read_file(fakeFilePath,-1)
        normal, labels = self.read_file(normalFilePath, 1)
        data, label = self.shuffle_data(fake_data + normal, fake_label + labels)
        normal_data, normal_label = self.stepOne_model(np.array(data), np.array(label), normal)
        X_train, x_test, Y_train, y_test = train_test_split(fake_data, fake_label, test_size=0.4, random_state=4)

        train_data = X_train + normal_data
        train_label = Y_train + normal_label
        train_data, train_label = self.shuffle_data(train_data, train_label)
        return train_data, train_label, fake_data, x_test, y_test

    def read_file_test(self, testFilePath):
        fake_data, fake_label = self.read_file(testFilePath, -1)
        return fake_data, fake_label

    def eval(self,pre_label, test_label):
        num = len([index for index in range(len(test_label)) if test_label[index] == pre_label[index]])
        return num / float(len(test_label))

    def eval_one(self, pre_label, test_label):
        num = len([index for index in range(len(test_label)) if pre_label[index] == 1])
        return num / float(len(test_label))

    def model_maker(self):
        print "Model Training Begin"
        train_data, train_label, fake_data, x_test, y_test = self.read_file_train(SimModel.fakeFilePath, SimModel.normalFilePath)
        names = ["NaiveBayesian","LogisticRegression","KNeighborsClassifier","RandomForest","OneClassSVM","SVC"]
        # names = ["RandomForest"]
        # '''
        # if sys.argv[4] == '3':
        #     models = [LogisticRegression(penalty='l1', C=100.0, tol=0.0001), tree.DecisionTreeClassifier(min_samples_leaf=1,min_samples_split=4), RandomForestClassifier(n_estimators=10, max_depth=None, max_features=4, oob_score=False,criterion= 'gini',min_samples_split=4,random_state=531)]
        # if sys.argv[4] == '2':
        #     models = [LogisticRegression(penalty='l1', C=0.001, tol=0.0001), tree.DecisionTreeClassifier(min_samples_leaf=1,min_samples_split=2),RandomForestClassifier(n_estimators=10, max_depth=None, max_features=4, oob_score=False,criterion= 'gini',min_samples_split=2,random_state=531)]
        # else:
        # '''
        # models = [GaussianNB(),
        #           LogisticRegression(penalty='l1', C=100.0, tol=0.0001),
        #           KNeighborsClassifier(n_neighbors = 50),
        #           RandomForestClassifier(n_estimators=30),
        #           svm.OneClassSVM(kernel="rbf", gamma=0.01),
        #           SVC(kernel="rbf", gamma=0.01)
        #           ]
        models = [
                  GaussianNB(),
                  LogisticRegression(penalty='l1', C=100.0, tol=0.0001),
                  KNeighborsClassifier(n_neighbors = 50),
                  RandomForestClassifier(n_estimators=10, max_depth=None, max_features= "auto", oob_score=False,criterion= 'gini'),
                  svm.OneClassSVM(kernel="rbf", gamma=0.01),
                  SVC(kernel="rbf", gamma=0.01)
                  ]


        # models = [LogisticRegression(penalty='l1', C=100.0, tol=0.0001), tree.DecisionTreeClassifier(min_samples_leaf=1,min_samples_split=4), RandomForestClassifier(n_estimators=10, max_depth=None, max_features=4, oob_score=False,criterion= 'gini',min_samples_split=4,random_state=531)]
        # models = [LogisticRegression(penalty='l1', C=0.001, tol=0.0001), tree.DecisionTreeClassifier(min_samples_leaf=1,min_samples_split=2),RandomForestClassifier(n_estimators=10, max_depth=None, max_features=4, oob_score=False,criterion= 'gini',min_samples_split=2,random_state=531)]

        for model, name in zip(models, names):
            if name == "OneClassSVM":
                fake_data = StandardScaler().fit_transform(fake_data)
                x_test = StandardScaler().fit_transform(x_test)
                model.fit(fake_data)
                y_pre = model.predict(x_test)
                accuracy = self.eval_one(y_pre, y_test)
                print name +" model accuracy is " , accuracy
            elif name == "SVC":
                train_data = StandardScaler().fit_transform(train_data)
                x_test = StandardScaler().fit_transform(x_test)
                model.fit(train_data, train_label)
                y_pre = model.predict(x_test)
                accuracy = self.eval(y_pre, y_test)
                print name +" model accuracy is " , accuracy
            else:
                model.fit(train_data, train_label)
                y_pre = model.predict(x_test)
                accuracy = self.eval(y_pre, y_test)
                print name +" model accuracy is " ,accuracy
            joblib.dump(model, "./models/" + name + ".m")
            print "model ==>" + name + " saved to dir whose path is ./models/"
        return models

    def model_reuse(self, models, test_data, test_label):
        accuracy = {}
        for mod in models:
            print str(mod)
            clf = joblib.load( "C:\Users\Marina\PycharmProjects\model\models/"+mod)
            print clf
            if 'Bayesian' in str(mod):
                NB_y_pre = clf.predict(test_data)
                accuracy_NB = self.eval(NB_y_pre, test_label)
                accuracy["accuracy_NB"] = accuracy_NB
                print "accuracy_NB", accuracy_NB

            if 'LogisticRegression' in str(mod):
                lr_y_pre = clf.predict(test_data)
                accuracy_lr = self.eval(lr_y_pre, test_label)
                accuracy["accuracy_lr"] = accuracy_lr

            if 'KNeighborsClassifier' in str(mod):
                knn_y_pre = clf.predict(test_data)
                accuracy_knn = self.eval(knn_y_pre, test_label)
                accuracy["accuracy_knn"] = accuracy_knn

            if 'Random' in str(mod):
                rf_y_pre = clf.predict(test_data)
                accuracy_rf = self.eval(rf_y_pre, test_label)
                accuracy["accuracy_rf"] = accuracy_rf

            if 'OneClass' in str(mod):
                test_data = StandardScaler().fit_transform(test_data)
                onesvm_y_pre = clf.predict(test_data)
                accuracy_onesvm = self.eval_one(onesvm_y_pre, test_label)
                accuracy["accuracy_onesvm"] = accuracy_onesvm

            if 'SVC' in str(mod):
                test_data = StandardScaler().fit_transform(test_data)
                svc_y_pre = clf.predict(test_data)
                accuracy_svc = self.eval(svc_y_pre, test_label)
                accuracy["accuracy_svc"] = accuracy_svc

        return accuracy

if __name__ == "__main__":
    model = SimModel()
    model.model_maker()
    path = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    files = os.listdir(path)
    test_data, test_label = model.read_file_test(SimModel.testFilePath)
    accuracy = model.model_reuse(files, test_data, test_label)
    print accuracy
