**Question 1. (6 points) Briefly describe in prose your solution, both the pairs and stripes implementation. For example: how many MapReduce jobs? 
What are the input records? What are the intermediate key-value pairs? What are the final output records? A paragraph for each implementation is about the expected length.**

Answer:

1. Pairs solution:
    There are two MapReduce jobs. 
    The input records are 122458.
    The intermediate key-value pairs are 975160.
    The final output records are 6325804.
    
2. Stripes solution:
    There are two MapReduce jobs. 
    The input records are
    The intermediate key-value pairs are 
    The final output records are
    
**Question 2. (2 points) What is the running time of the complete pairs implementation? What is the running time of the complete stripes implementation? 
(Tell me where you ran these experiments, e.g., linux.student.cs.uwaterloo.ca or your own laptop.)**

Answer:

1. Pairs solution:
    24.635 seconds in linux.student.cs.uwaterloo.ca
    
2. Stripes solution:

**Question 3. (2 points) Now disable all combiners. What is the running time of the complete pairs implementation now? 
What is the running time of the complete stripes implementation? (Tell me where you ran these experiments, e.g., linux.student.cs.uwaterloo.ca or your own laptop.)**

Answer:

1. Pairs solution:
    25.674 seconds in linux.student.cs.uwaterloo.ca
    
2. Stripes solution:

**Question 4. (3 points) How many distinct PMI pairs did you extract? Use -threshold 10.**

Answer:

77198


**Question 5. (6 points) What's the pair (x, y) (or pairs if there are ties) with the highest PMI? Use -threshold 10. 
Similarly, what's the pair with the lowest (negative) PMI? Use the same threshold. Write a sentence or two to explain these results.**

Answer:

The pair with the highest PMI is (are, daughter)(i.e. (daughter, are)). 
This means that the more information you know about the probability of seeing daughter having just seen are. 
That is to say they are cooccur more frequently than would be expected with the assumption that the occurrence of two words are independent.

The pair with the lowest PMI is (it, sake)(i.e. (sake, it))
This means that the less information you know about the probability of seeing sake having just seen it. 
That is to say they are cooccur less frequently than would be expected with the assumption that the occurrence of two words are independent.

**Question 6. (6 points) What are the three words that have the highest PMI with "tears" and "death"? -threshold 10. And what are the PMI values?**

Answer:

The three words that have the highest PMI with "tears" are "shed", "salt", and "eyes".
The three words that have the highest PMI with "death" are "father's", "die", and "life".

**Question 7. (5 points) In the Wikipedia dataset, what are the five words that have the highest PMI with "hockey"? And how many times do these words co-occur? Use -threshold 50.**

Answer：

The five words that have the highest PMI with "hockey" are "defenceman", "winger", "sledge", "goaltender", and "ice".

***Question 8. (5 points) Same as above, but with the word "data".***

Answer:

The five words that have the highest PMI with "data" are "cooling", "encryption", "array", "storage", and "database".