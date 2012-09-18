#!/usr/bin/python

from collections import defaultdict
import sys, re
import nltk
import json
import operator
from lepl.apps.rfc3696 import Email, HttpUrl

def log(message):
  try:
    sys.stderr.write(__file__ + ": " + message + "\n")
  except:
    sys.stderr.write(message + "\n")

class TokenExtractor:
  
  def __init__(self):
    self.setup_lepl()
    
  def setup_lepl(self):
    self.is_url = HttpUrl()
    self.is_email = Email()
  
  def remove_stop_words(self, tokens):
    words = list()
    for token in tokens:
      if self.stop_words.has_key(token):
        pass
      else:
        words.append(token)
    return words
  
  def tokenize(self, status):
    return nltk.word_tokenize(status)
  
  def lower(self, tokens):
    words = list()
    for token in tokens:
      words.append(token.lower())
    return words
  
  def remove_punctuation(self, tokens):
    punctuation = re.compile(r'[-.@&$#`\'?!,":;()|0-9]')
    words = list()
    for token in tokens:
      word = punctuation.sub("", token)
      if word != "":
        words.append(word)
    return words
  
  def short_filter(self, tokens):
    words = list()
    for token in tokens:
      if len(token) > 2:
        words.append(token)
    return words
  
  # Do a regex later
  def remove_urls(self, tokens):
    words = list()
    for token in tokens:
      if self.is_url(token):
        pass
      else:
        words.append(token)
    return words

def main():
  te = TokenExtractor()
  for line in sys.stdin:
    tokens = te.tokenize(line)
    #print tokens
    no_urls = te.remove_urls(tokens)
    #print no_urls
    lowers = te.lower(no_urls)
    #print lowers
    no_punc = te.remove_punctuation(lowers)
    #print no_punc
    no_shorts = te.short_filter(no_punc)
    print no_shorts

if __name__ == "__main__":
    main()