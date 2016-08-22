package nikolai

import "testing"

func TestABC(t *testing.T) {
	n, err := Open("data", ":6380", "")
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
}
