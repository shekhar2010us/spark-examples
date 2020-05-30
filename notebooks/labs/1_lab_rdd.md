## This file contains several exercise on Spark RDD

#### Time: 120 minutes


Input file: `/home/ec2-user/data/blogtexts`


### Question 1

```
- Read the file "blogtexts" in a rdd
- Convert all words in a rdd to lowercase and split the lines of a document using space.
```

### Question 2

```
Take output from question 1
- convert list of lists into one list ==> flatten the list
```

### Question 3

```
Take output from question 2
- remove stopwords from the corpus, where stopwords = ['is','am','are','the','for','a']
- tokens should only have alphabets and numbers
```

### Question 4

```
Take output from question 3
- group the words based on first 3 characters
- compare the count of this rdd and the previous rdd
- print few records
```


### Question 5

```
Take output from any question
- For all words, find the number of times it appeared in the corpus
- sort the result by words having highest frequency (descending by frequency)
- check number of times the word "spark" appears in the corpus
```


### Question 6

```
Take output from question 3
- check number of partitions in the output from question 3
- re-partition to 10
- count the frequency of word "spark" in each of the 10 partitions
```


### Question 7

```
Take output from question 5
- Take 50% random sample - sample1
- Take 50% random sample - sample2
- take "union" of two RDDs
- "join" both RDDs
```


### Question 8

```
- Create a RDD using a number array
- Compute the average
```


### Question 9

```
- Create a RDD using a number array
- Compute the standard deviation
```


### Question 10

```
- Create a RDD using a number array (1000 numbers)
- Create your own sampler, and sample 30%
     ** do not use Spark default takeSample()
- Run multiple times and check size of the sample
```
