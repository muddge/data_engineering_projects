

```python
from datetime import datetime
import json
import io
import string
import csv

import psycopg2

from pipeline import build_csv, Pipeline
from stop_words import stop_words

pipeline = Pipeline()

# Task to parse JSON file into python dictionary object

@pipeline.task()
def file_to_json():
    with open('hn_stories_2014.json', 'r') as f:
        data = json.load(f)
        stories = data['stories']
    return stories

# Task to filter stories that have more than 50 pts, more than 1 comments
# and do not begin with "Ask HN"

@pipeline.task(depends_on=file_to_json)
def filter_stories(stories):
    def is_popular(story):
        return story['points'] > 50 and story['num_comments'] > 1 and not story['title'].startswith('Ask HN')
    
    return (
        story for story in stories
        if is_popular(story)
    )

# Task to generate CSV file with the correct headers

@pipeline.task(depends_on=filter_stories)
def json_to_csv(stories):
    lines = []
    for story in stories:
        lines.append(
            (story['objectID'], datetime.strptime(story['created_at'], "%Y-%m-%dT%H:%M:%SZ"), story['url'], story['points'], story['title'])
        )
    return build_csv(lines, header=['objectID', 'created_at', 'url', 'points', 'title'], file=io.StringIO())

# Task to generate each news story title

@pipeline.task(depends_on=json_to_csv)
def extract_titles(csv_file):
    reader = csv.reader(csv_file)
    header = next(reader)
    idx = header.index('title')
    
    return (line[idx] for line in reader)

# Task to make title lower case and remove punctuation characters from title

@pipeline.task(depends_on=extract_titles)
def clean_title(titles):
    for title in titles:
        title = title.lower()
        title = ''.join(c for c in title if c not in string.punctuation)
        yield title

# Task to remove stop words, and return dictionary of word frequency
# of all news story titles
        
@pipeline.task(depends_on=clean_title)
def build_keyword_dictionary(titles):
    word_freq = {}
    for title in titles:
        for word in title.split(' '):
            if word and word not in stop_words:
                if word not in word_freq:
                    word_freq[word] = 1
                word_freq[word] += 1
    return word_freq

# Task to return top 100 word frequencies

@pipeline.task(depends_on=build_keyword_dictionary)
def top_keywords(word_freq):
    freq_tuple = [
        (word, word_freq[word])
        for word in sorted(word_freq, key=word_freq.get, reverse=True)
    ]
    return freq_tuple[:100]

ran = pipeline.run()
top_keywords = ran[top_keywords]
print(top_keywords)

# Code to add word frequencies to postgres DB

# conn = psycopg2.connect('dbname=dq user=dq')
# cur = conn.cursor()

# cur.execute("DROP TABLE IF EXISTS word_freq")

# cur.execute("CREATE table word_freq( \
          #   word text, \
          #   freq integer);")

# for row in top_keywords:
   #  cur.execute("INSERT into word_freq(word, freq) VALUES (%s, %s)", row[0], row[1])

# conn.commit()
# conn.close()
    
```

    [('new', 186), ('google', 168), ('bitcoin', 102), ('open', 93), ('programming', 91), ('web', 89), ('data', 86), ('video', 80), ('python', 76), ('code', 73), ('facebook', 72), ('released', 72), ('using', 71), ('javascript', 66), ('2013', 66), ('free', 65), ('source', 65), ('game', 64), ('internet', 63), ('microsoft', 60), ('c', 60), ('linux', 59), ('app', 58), ('pdf', 56), ('language', 55), ('work', 55), ('2014', 53), ('software', 53), ('startup', 52), ('make', 51), ('apple', 51), ('use', 51), ('yc', 49), ('security', 49), ('time', 49), ('github', 46), ('nsa', 46), ('windows', 45), ('world', 42), ('like', 42), ('way', 42), ('computer', 41), ('heartbleed', 41), ('project', 41), ('1', 41), ('git', 38), ('design', 38), ('ios', 38), ('dont', 38), ('users', 38), ('ceo', 37), ('life', 37), ('developer', 37), ('twitter', 37), ('vs', 37), ('os', 37), ('day', 36), ('big', 36), ('online', 35), ('android', 35), ('simple', 34), ('years', 34), ('court', 34), ('apps', 33), ('browser', 33), ('guide', 33), ('mt', 33), ('api', 33), ('says', 33), ('learning', 33), ('fast', 32), ('problem', 32), ('mozilla', 32), ('engine', 32), ('firefox', 32), ('server', 32), ('gox', 32), ('site', 32), ('amazon', 31), ('year', 31), ('introducing', 31), ('built', 30), ('text', 30), ('million', 30), ('support', 30), ('better', 30), ('people', 30), ('stop', 30), ('does', 29), ('development', 29), ('3', 29), ('tech', 29), ('2048', 28), ('library', 28), ('developers', 28), ('website', 28), ('inside', 28), ('chrome', 28), ('just', 28), ('did', 28)]

