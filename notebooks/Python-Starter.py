# Databricks notebook source
print ("Hello World")

# COMMAND ----------

# comments

# COMMAND ----------

# variable , datatypes
# dynamic typing language, no compile time check
# has runtime type

name = "Python"
year = 1995
exp  = 4.5
graduate = True 

print (name, type(name) )
print (year, type(year) )
print (exp, type(exp) )
print (graduate, type(graduate) )

# COMMAND ----------

# duck duck typing
name = "Python"
print(name, type(name), name.upper())
name = 12
# let it crash, due to type int, doesn't has upper function
print(name, type(name), name.upper() ) # exception and crash

# COMMAND ----------

name = 'Python'
name = "Python"
# multi line string, preserve space, SPECIAL Char like TAB, SPACE etc
name = """
Python
"""

# if you don't like # comment put for each line, use document string format which is """ """ for multi lines

"""
Hi, I am learning Python
In Databricks
I will learn spark too
Synpase + Azure next day
"""

# COMMAND ----------

# Python and BLOCK using :, we use Indentation  SPACE,  TAB, DONT' MIX
# Use SPACES, configure your editor, to convert TAB to SPACE
# in other languages, {   }, begin and end , Python uses :
 #databricsk convert the TAB key into SPACEs
if (True):
  print("all is well")
  pass # dummy statement, does nothing, to represent todo, or incomplete code portion
  print ("block statement")

# COMMAND ----------

# if statement, that doesn't return output
n = 11
if (n % 2 == 0):
  print("Even")
  print(n)
else:
  print ("Odd")
  print(n)

# COMMAND ----------

# if and else expression, that returns output
n = 11
result = "Even" if (n % 2 == 0) else "Odd"
print(result)

# COMMAND ----------

# functions, or also know methods
# reusable code
def add(a, b):
  print (a, b)
  return a + b

print(add(10, 20)) # paramters passed left to right, a = 10, b = 20
# named argument/parameter
print(add(a = 10, b = 20)) # parameters passed by name, order doesn't matter   a = 10, b = 20
print(add(b = 20, a = 10)) # a = 10, b = 20
print(add (10, b= 20)) # mixed, without name are passed left to right, named one after that.. a = 10, b = 20

# COMMAND ----------

# default values used when parameter not passed as value
def sub(a, b = 0):
  print(a, b)
  return a - b

print (sub (10, 5) )  # a = 10, b = 0
print (sub(10)) # b is defaulted to 0 , a = 10,  b= 0
print (sub(10, b = 5)) # a = 10, b = 5


# COMMAND ----------

# variable number of arguments
#  a function that accept 1 or 2 or many paramters.. 
# the arguments passed as collection
# numbers is tuple type
def sum(*numbers):
  print (type(numbers))
  print (numbers)
  
  s = 0
  
  for n in numbers:
    s += n
    
  return s
  
# the arguments are passed as tuple/collection
print ( sum (10) )  # 1 arg
print ( sum () )  # no arg
print ( sum (10,20,30) )  # 3 args



# COMMAND ----------

# in Python, functions are called as first class citizen
# They treated equally as like variable or types
# create, return, assign a function to another variable, pass func as argument
# function
def power(n):
  return n * n

# annonymous function, called lambda , one line function, typically expression like
# no name for the function
# arguments: expression
pow = lambda n: n * n

print (power(5))
print (pow(5))

print (power(n=5))
print (pow(n=5))

# citizen? it is an object, it has type, both pritn function
print ( type(power) ) # function, has a name
print (type (pow)) # function, no name

print (type(power), power.__name__) # function, power
print (type(pow), pow.__name__) # function, <lambda> no specific name


# COMMAND ----------

# higher order function
# a function that accept another function as input: in python, functions are object, we can pass them as arg, return as value

def square(n):
  print ('sq called', n)
  return n * n

# scenario: give me sum of square of the number
# scenario: give me sum of the numbers
# scenario: give me sum of cube of the number
# func, is a reference to either def/lambda, a function
# *numbers, number tuple, contains 0, or more elements
def sum(func, *numbers):
  s = 0
  
  for n in numbers:
    # func can be any function that takes a number as arg, return a value
    s += func (n) # n is an abstraction, the internal details are not cared by sum function
    
  return s

print ( sum (square, 1, 2, 3, 4)) # print 30 , 1 * 1 , 2 * 2 , 3 * 3, 4 * 4

#  scenario: give me sum of the square of numbers
# lambda , with square
print (sum(lambda n: n * n, 1, 2, 3, 4))  # print 30 now lamda has no name, composed inlining, a quick 1 line function
# scenario: give me sum of cube of the number
print (sum(lambda n: n * n * n, 1, 2, 3, 4)) # cube, 100
  
# scenario: give me sum of the numbers, idendity pattern
print (sum(lambda n: n, 1, 2, 3, 4)) # 10, sum of numbers

# COMMAND ----------

# a customer may give a csv file
# remove extra space around line,  lambda line: line.strip()
# delimit using & not a comma
# line ending ;;

# COMMAND ----------

# multi line statements
# python will treat each line as EoL - end of line by default
# b = 10 * 20
"""
# will not work
b = 10 
*
20

b = 10  *
20
"""

# COMMAND ----------

# multi line continuation using \ [Option 1]
# \NOSPACE
b = 10\
*\
20

print(b)

# COMMAND ----------

# multi line continuation using paranthesis, only for expression, single expression, not a replacement indentdation
b = (
10 
*
20
)

print(b)

# COMMAND ----------

# data structure
# list, Mutable, we can change the list values, append, or delete
# not a good one for concurrent, parallel applications

# list, a collection/seq of elements

numbers = [10, 20, 30, 40]
names = ["python", "java", "jvm", "sql", "spark"]

print (len(names))
print (len(numbers))

print (names)
# chage one member by index
names[1] = "scala" # now java replaced with scala
print (names)

# append an element/memebr
names.append("pyspark")
print(names)

names.remove("scala") # remove a member

print (names)

# COMMAND ----------

names = ["python", "java", "jvm", "sql", "spark"]

# accessor using Index , position of the element, 0, 1, 2, 3,...

# index position from left to right
print(names[0]) # python
print(names[3]) # sql

print ("-" * 20)
# index position from right represented using - (minus) negative number
print (names[-1]) # spark
print(names[-3]) # jvm
print ("-" * 20)


# COMMAND ----------

names = ["python", "java", "jvm", "sql", "spark"]

# [lower: upper], includ lower, until upper exclude the upper vale, ie upper - 1
# range
# start from 0, until end, including last element
print ( names[0:]) # prints all
# start from 0, until 2nd index, 2 will not return, 0 and 1 elements will printed
print (names[0:2]) # python, java
print (names[0:-1]) # -1 is spark, that will be returned, rest all shall be printed 


# COMMAND ----------

# tuple, collection of elements
# immutable
# from api, it is same like list, but we cannot change, delete, add members, READ ONLY, IMMUTABLE
# good for big data
# represented using ()
# for 1 member tuple, use ,
t0 = (10) ## NOT TUPLE, instead INT Type
t1 = (10, )
t2 = (10, 20, 30)

# t0 is not a tuple

print (print (type(t0)))
print (print (type(t1)))
print (print (type(t2)))

print (len(t2))
print (t2[0:])
print(t2[:-1])

# will error
#t2[1] = 100

# COMMAND ----------

# map/dictionary
# {}, key: value

# the keys are TN, KA and vlaues are Tamil Nadu, Karnataka
states = {
  'TN': 'Tamil Nadu',
  'KA': 'Karnataka'
}

print(states)
print(states["TN"])

states["KL"] = "Kerala"

print(states)

del states['KL']

print(states)

# COMMAND ----------


