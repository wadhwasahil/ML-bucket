import os
import sys
from sklearn.feature_extraction.text import TfidfVectorizer

a = set()
b = set()
ans = dict()
n = int(raw_input())
data = sys.stdin.readlines()
vectorizer = TfidfVectorizer(min_df=1)
tf_idf = vectorizer.fit_transform(data)
sim_matrix = tf_idf * tf_idf.T
val = []
for i in range(n):
	for j in range(n):
		val.append((-sim_matrix[i, n+1+j], i, j))
val = sorted(val)
for v, i, j in val:
	if i in a or j in b:
		continue
	a.add(i)
	b.add(j)
	ans[i] = j
for i in range(n):
	print ans[i] + 1
