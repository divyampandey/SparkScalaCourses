import org.omg.CORBA.Any
//Functions in Scala
def squareIt(x : Int) : Int = {
  x * x
}
println(squareIt(5))
def cubeIt(x: Int): Int = x * x * x
println(cubeIt(5))

def printfunction(str: String) = println(str)

printfunction("hahahahahha")

//lamda or anonymous functions inline functions

def transformInt(x:Int , f: Int=>Int)={
  f(x)
}
transformInt(3,squareIt)

//function to convert to uppercase

def ToUpperCase(x : String ): String = {
  x.toUpperCase()
}

ToUpperCase("hello i am divyam")
ToUpperCase("this is 10")

def uppercase(x:String, f:String=>String)=f(x)

uppercase("Hello",x=>x.toUpperCase())

{val x = "divyam"; x.toUpperCase()}
