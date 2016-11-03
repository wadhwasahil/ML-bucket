import pandas as pd
from sklearn import svm
from sklearn.preprocessing import OneHotEncoder

faltu_cols = ["System-Id", "Message", "drug-Id", "sideEffect-Id"]
numerical_cols = ["distance", "drugsInBetween", "seInBetween", "sentencesInBetween"]
print "Loading data..."
train_dataframe = pd.read_csv("train.csv")
for col in faltu_cols:
	del train_dataframe[col]
num_array = pd.DataFrame()
for col in numerical_cols:
	num_array = pd.concat([num_array, train_dataframe[col]], axis=1)
	del train_dataframe[col]
y_temp = train_dataframe.relType
y = [1 if val == "valid" else 0 for val in y_temp]
print len(y)
del train_dataframe["relType"]
cols = train_dataframe.columns
X = pd.get_dummies(train_dataframe)
X = pd.concat([X, num_array], axis=1)
print "Data loaded..."
classifier = svm.SVC(kernel='linear', C=1)
# X = X.reshape(X.size, 1)
print "Training started..."
classifier.fit(X, y)
print "Training ended..."
results = classifier.predict(X)
num_correct = (results == y).sum()
recall = num_correct / len(y)
print "model accuracy (%): ", recall * 100, "%"
