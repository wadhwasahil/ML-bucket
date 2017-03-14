from gensim import models
from nltk.tokenize import TweetTokenizer
import os

file_name = "small_text.txt"
dir_name = "doc2vec_models"
tokeninzer = TweetTokenizer()


def is_word(word):
    for char in word:
        if char.isalpha() or char.isdigit():
            return True
    return False


# TODO stemming
class LabeledLineSentence(object):
    def __init__(self, filename):
        self.filename = filename

    def __iter__(self):
        for uid, line in enumerate(open(file_name)):
            words = [word for word in tokeninzer.tokenize(line) if is_word(word)]
            yield models.doc2vec.TaggedDocument(words=words, tags=['SENT_%s' % uid])


docs = LabeledLineSentence(file_name)
print("Training doc2vec model.......................")
model = models.Doc2Vec(docs, size=100, alpha=0.025, min_alpha=0.025, min_count=1, iter=30, workers=4)
print("Model trained................................")
if not os.path.isdir(os.path.join(os.getcwd(), dir_name)):
    os.makedirs(dir_name)
model.save(dir_name + "/" + "model_1")
