package nikolai

import (
	"fmt"
	"reflect"
	"testing"
	"unsafe"
)

func TestABC(t *testing.T) {

	n, err := Open("data-test-0", ":6380", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	select {}
}
func TestStringCopy(t *testing.T) {
	var data = make([]byte, 0)
	data = append(data, []byte("HELLO WORLD")...)

	var strA = string(data)

	fmt.Printf("strA '%s'\n", strA)

	bytA := []byte(strA)
	bytA[0] = 'J'

	fmt.Printf("bytA '%s'\n", bytA)
	fmt.Printf("strA '%s'\n", strA)

	return
	var bytesA = []byte("HELLO WORLD")
	fmt.Printf("strA   : '%v': %d\n", strA, (*(*reflect.StringHeader)(unsafe.Pointer(&strA))).Data)
	fmt.Printf("bytesA : '%v': %d\n", string(bytesA), (*(*reflect.SliceHeader)(unsafe.Pointer(&bytesA))).Data)

	var strB = string(bytesA)
	var bytesB = []byte(strA)

	fmt.Printf("strB   : '%v': %d\n", strB, (*(*reflect.StringHeader)(unsafe.Pointer(&strB))).Data)
	fmt.Printf("bytesB : '%v': %d\n", string(bytesB), (*(*reflect.SliceHeader)(unsafe.Pointer(&bytesB))).Data)

}

func BenchmarkStringBytesEqual(t *testing.B) {
	var data = make([]byte, 0)
	data = append(data, []byte("HELLO WORLD")...)
	var str = "HELLO WORLD"
	for i := 0; i < t.N; i++ {
		if string(data) != str {
			t.Fatal("error")
		}
	}
}
func equalCall(str string, data []byte) bool {
	return string(data) == str
}

func BenchmarkStringBytesEqualCall(t *testing.B) {
	var data = make([]byte, 0)
	data = append(data, []byte("HELLO WORLD")...)
	var str = "HELLO WORLD"
	for i := 0; i < t.N; i++ {
		if !equalCall(str, data) {
			t.Fatal("error")
		}
	}
}

func BenchmarkStringBytesEqualCallConvert(t *testing.B) {
	var data = make([]byte, 0)
	data = append(data, []byte("HELLO WORLD")...)
	var str = "HELLO WORLD"
	for i := 0; i < t.N; i++ {
		if !equalCall(str, []byte(str)) {
			t.Fatal("error")
		}
	}
}
