// values and variables
// values are scala way of writing the code i.e more functional the reason being it allows for better distributed programming.
// to define a val

val intone : Int = 1
print(intone)
val stringone : String = "one"
print(stringone)
var stringtwo : String = stringone + " + one"
print(stringtwo)

// Data Types in Scala


val longnumber: Long = 1234455566
val booleanval: Boolean = true
val floatnum : Float = 2343.343f
val decimalnum : Double = 2343.3343
val smallnum : Byte = 127

// String formatting

println(f"the integer is $intone and boolean is $booleanval and float is $floatnum")
println(f"the expression is ${5*4}")
println(f"to round out the decimal and float use percent dot notation $floatnum%.2f")

// Regular expression

val ultimatestring : String = "Everything in this world is temporary and 42"

val pattern = """.* ([\d]+).*""".r

val pattern(answerstring) = ultimatestring

println(pattern.getClass)
println(decimalnum.getClass)
println(booleanval.getClass)
println(intone.getClass)

// logical operations

val isgreater =  1 > 2
val islesser =   1 < 2
println(isgreater)
println(islesser)
val impossible  = isgreater && islesser
println(impossible)

val maybe = isgreater || islesser
print(maybe)

//padding in scala


// small program  scala.io.StdIn.readInt() to take value from user.
//.readFloat()
//.readLine()
//.readByte() etc are few other methods.
println("Double value of pi in floating point: ")
val pi = 3.1453435
val doublepi = pi * 2
print(f" Double of pi upto 3 decimal precision is $doublepi%.3f")
