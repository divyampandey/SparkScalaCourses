//Flow control

if (1 > 3) println("Impossible") else println("World makes sense !")

if (1 > 3) {
  println("Impossible")
  println("Really !!")
}
else {
  println("WOrld makes sense !")
  println("still...")
}


// Matching
val number = 3

number match{
  case 1 => println("Number one ")
  case 2 => println("Number two ")
  case 3 => println("Number three")
  case _ => println("DOn't know then .. ")
}


// for loops

for (x <- 1 to 4){
  val num = x * x
  println(num)
}

// while loops

var x = 10
while (x >= 0 ){
  println(x)
  x -=1
}

// do while loop
x = 0
do {
  println(x)
  x +=1
}
while(x<=10)

// Expression

{ val x = 10; x + 20}
println({val x = 20 ; x + 80})


// Exercise print Fibonacci series
var numbers = 20
var first_num = 0
var second_num = 1
var Fibonacci_list = List(first_num,second_num)
while(numbers!=0){
  val third_num = first_num+second_num
  Fibonacci_list = Fibonacci_list :+ third_num
  first_num=second_num
  second_num = third_num
  numbers -=1
}
//traversing a list
for(i<- Fibonacci_list){
  println(i)
}
// traversing a list using foreach
Fibonacci_list.foreach(println)

// using foreach for other fun things
var sum = 0
Fibonacci_list.foreach(sum += _)

println(sum)

// list comprehension in Scala

for(value <- Fibonacci_list if value%2==0) println(value)