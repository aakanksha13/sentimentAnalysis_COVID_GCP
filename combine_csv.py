import csv
inputs = ["2020-03-29 Coronavirus Tweets.CSV", "2020-04-03 Coronavirus Tweets.CSV",\
 "2020-04-08 Coronavirus Tweets.CSV", "2020-04-13 Coronavirus Tweets.CSV", "2020-03-30 Coronavirus Tweets.CSV",\
 "2020-04-04 Coronavirus Tweets.CSV", "2020-04-09 Coronavirus Tweets.CSV"\
,"2020-04-14 Coronavirus Tweets.CSV", "2020-03-31 Coronavirus Tweets.CSV", "2020-04-05 Coronavirus Tweets.CSV" \
,"2020-04-10 Coronavirus Tweets.CSV", "2020-04-15 Coronavirus Tweets.CSV", "2020-04-01 Coronavirus Tweets.CSV" \
,"2020-04-06 Coronavirus Tweets.CSV", "2020-04-11 Coronavirus Tweets.CSV", "2020-04-02 Coronavirus Tweets.CSV"\
, "2020-04-07 Coronavirus Tweets.CSV", "2020-04-12 Coronavirus Tweets.CSV"]  # etc
# First determine the field names from the top line of each input file
# Comment 1 below
fieldnames = []
for filename in inputs:
  with open(filename, "r", newline="") as f_in:
    reader = csv.reader(f_in)
    headers = next(reader)
    for h in headers:
      if h not in fieldnames:
        fieldnames.append(h)
# Then copy the data
with open("coronatweets.csv", "w", newline="") as f_out:   # Comment 2 below
  writer = csv.DictWriter(f_out, fieldnames=fieldnames)
  for filename in inputs:
    with open(filename, "r", newline="") as f_in:
      reader = csv.DictReader(f_in)  # Uses the field names in this file
      for line in reader:
        # Comment 3 below
        writer.writerow(line)
