package main

/*
#include <stdio.h>
static void hello(void) { printf("ctest: cgo ok\n"); }
*/
import "C"

func main() {
	C.hello()
}
