// Define an implicit class to enrich the Int class
implicit class a(x: Int) {
    def squared: Int = x * x
    def myCubed: Int = x.squared * x
}
type MyInt = Double
implicit def myToInt(s:Char): Int = (s - '0')

// Use the implicit class to call the squared method on an Int
val number: MyInt = '5'
// val squaredNumber = number.squared

5.squared

trait A
class B extends A

trait C
class D extends C

def myFun[T: B: D] = println("It works")

'5'-'0'