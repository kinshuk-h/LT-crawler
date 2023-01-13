import regex
import nltk.data
import nltk.corpus
import nltk.tokenize

from .base import Filter

class SentenceCountFilter(Filter):
    """ Filters paragraphs over number of sentences. """

    name = "sent_count"

    def __init__(self) -> None:
        super().__init__()

        try:
            nltk.data.find("tokenizers/punkt.zip")
        except LookupError:
            nltk.download("punkt")

        try:
            nltk.data.find("corpora/stopwords")
        except LookupError:
            nltk.download("stopwords")

        self.tokenizer = nltk.data.load("tokenizers/punkt/english.pickle")
        self.stopwords = { *nltk.corpus.stopwords.words("english") }

    @classmethod
    def get_option_list(cls):
        return [
            dict(name="min_sents", help="minimum number to sentences that each paragraph must have", default=3),
            dict(name="min_words", default=30,
                 help=(
                    "minimum number of words that each paragraph must have (excluding stopwords) to "
                    "bypass the sentence count requirement. Defaults to 30"
                 )),
            dict(name="tokenizer_path", default=None,
                 help="path to the sentence tokenizer to use (must be compatible with NLTK)")
        ]

    def refresh_state(self):
        if self.options['tokenizer_path'] is not None:
            self.tokenizer = nltk.data.load(self.options['tokenizer_path'])

    def load(self, paragraph):
        sentences = [ *self.tokenizer.tokenize(paragraph) ]
        words = [
            word for sentence in sentences for word in nltk.tokenize.word_tokenize(sentence)
            if word.lower() not in self.stopwords and regex.search(r"(?ui)\p{L}+", word)
        ]
        return sentences, words

    def decision(self, paragraph_rep):
        sentences, words = paragraph_rep
        return len(sentences) >= int(self.options['min_sents']) \
            or len(words) >= int(self.options['min_words'])