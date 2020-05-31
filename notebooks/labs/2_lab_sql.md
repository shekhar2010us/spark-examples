## This file contains several exercise on Spark SQL

#### Time: 120 minutes


Input files: 
`/home/ec2-user/data/train.csv` and `/home/ec2-user/data/test.csv`


### Question 1

```
- Load two csv files in two dataframes (train, test)
- print datatypes of columns
- print first 10 observations from both df
- count rows in both df
```


### Question 2

```
- List columns in train and test df
- Compare the columns of the two df, cite the difference
```


### Question 3

```
- Check summary statistics of the two dataframes
- Check summary statistics of the extra column
- Check how many times the extra column in train is null
```


### Question 4

```
- Create a report of `all columns` - counting number of records they are either NULL or NaN
    - in both dataframes (train and test)
The report should be in Pandas dataframe, preferably
```


### Question 5

```
- Select subset of columns from train df: 'User_ID' and 'Age'
- Check distinct Age in train and test files
- collect all distinct ages (from train and test) in a list and sort the list
- Add a column to both dataframes -> convert ages into categories (1,2,3,4,5,6,7)
```


### Question 6

```
EDA
- from train and test, remove record even if one column is null
- select columns ('Age','Gender', 'Marital_Status', 'Product_Category_3') to create another dataframe
    - remove duplicate rows
    - fill a constant number "-1" if there's a null or NaN
    - remove all null from base dataframe
    ---- check count in each step
```


### Question 7

```
- filter the rows in train which has Purchase more than 15000
- mean of purchase for each age group in train df
- count purchases by each age group
```


### Question 8

```
- create 40-60 split of the train df
- take union of the two splits
** can you explain why the count is more than original count of the dataframe
```


### Question 9

```
- create another column with purchase = purchase/2
- delete original column purchase
```


### Question 10

```
- register train df as temp table in spark sqlcontext
- check table in spark sqlcontext
- run sql queries on the temp table (few)
- delete table
- check table in spark sqlcontext
```
