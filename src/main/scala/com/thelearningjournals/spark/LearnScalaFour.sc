// Data Structures

// Tuples
// Immutable List
// In tuples the notation for accessing the elements is _.1, _.2, and so on...
// Point here to note is the indexing which starts from 1 unlike 0 .

val myFirstTuple = ("1", "Divyam", "scala")
println(myFirstTuple._1)

// Another point to note is the different types of elements that you can
// have inside the tuple

// printing tuple
// tuple extends Product so it can be iterated and printed using productiterator
myFirstTuple.productIterator.foreach(println)



// List

val myFirstList = List(1,2,3,4,5,6)
for(x <- myFirstList){println(x)}

myFirstList(0)
myFirstList(1)

// Indexing of List starts from 0 and it should contain
// element of same type only.

// Map - Reduce

// Map basically allows you to apply one function to every element of the
// iterator
// say if you want to find the lenght of every string present in your list
// to find the maximum length of string you can use map
// example

val movieList = List("Titanic","Godfather","Wolvarine","Harry Potter and the prisoner of Azkaban","Inception")

val lengthList = movieList.map((x:String)=>x.length)

// another way of writing the same thing

val lenList = movieList.map(_.length)
println(lenList)

// Reduce
// Reduce basically apply operation continously to the
//iterator until the final result is calculated
// and it's sort of apply the output again as input
// example
//suppose if i want to concatenate the movieList all together

val combineMovieList = movieList.reduce((x:String, y:String)=> x+y)

//Filter function
// It is used to filter out some values
val iHateLoveStory = movieList.filter(_.toLowerCase()!="titanic")

// List and some operations

val numberList = List(1,2,3,4,5)
numberList.sum
numberList.head
numberList.tail
val reversedList = numberList.reverse
reversedList.sorted
// concatenate 2  list
val duplicateList = numberList ++ numberList

val distinctList = duplicateList.distinct
val ifhasfive = distinctList.contains(5)

// Maps --- > also called dictionary in Python

val myFirstMap = Map("0"->"Divyam","1"->"Shefali","2"->"Rohit")

println(myFirstMap("0"))

val friend = util.Try(myFirstMap("3")) getOrElse("unknown")


for(z <- myFirstMap) {println(z)}